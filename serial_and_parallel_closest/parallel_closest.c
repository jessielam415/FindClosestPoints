#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "point.h"
#include "serial_closest.h"
#include "utilities_closest.h"


/*
 * Multi-process (parallel) implementation of the recursive divide-and-conquer
 * algorithm to find the minimal distance between any two pair of points in p[].
 * Assumes that the array p[] is sorted according to x coordinate.
 */
double closest_parallel(struct Point *p, int n, int pdmax, int *pcount) {
    int counter = 0;
    int r;
	int fdl[2];
    int fdr[2];
    if ((n < 4) || pdmax == 0) {
        return closest_serial(p, n);
    }
    //else if n >=4 && pdmax > 0

    // Find the middle point.
    int mid = n / 2;
    struct Point mid_point = p[mid];
    //create first pipe
    if (pipe(fdl) == -1) {
        perror("pipe");
        exit(1);
    }
    //Create first Child with the left half of the array
    r = fork();
    if (r == 0) {
        close(fdl[0]);
        double dl = closest_parallel(p, mid, pdmax - 1, pcount);
        close(fdl[0]);
        if (write(fdl[1], &dl, sizeof(double)) == -1) {
            perror("write to pipe");
            exit(1);
        }
        close(fdl[1]);
        exit(*pcount);
    } else if (r == -1) {
            perror("fork");
            exit(1);
    }
    //create second pipe
    if (pipe(fdr) == -1) {
	    perror("pipe");
        exit(1);
	}
    // Create second child deal with the right half of the array
    r = fork();
    if (r == 0) { 
        close(fdr[0]);
        double dr = closest_parallel(p + mid, n - mid, pdmax - 1, pcount);
        if (write(fdr[1], &dr, sizeof(double)) == -1) {
            perror("write to pipe");
            exit(1);
        };
        close(fdl[1]);
        exit(*pcount);
    }
    else if (r == -1) {
        perror("fork");
        exit(*pcount);
    }

    //wait for both child processes to complete
        for (int i = 0; i < 2; i++) {
            pid_t pid;
            int status;
            if( (pid = wait(&status)) == -1) {
                perror("wait");
            } else {
                if (WIFEXITED(status)) {
                    counter += WEXITSTATUS(status) + 1;
                } else {
                    exit(1);
                }
            }
        }
        double child_dl;
        if (read(fdl[0], &child_dl, sizeof(double)) < 0) {
                perror("read from pipe");
                exit(1);
        }
        double child_dr;
        if (read(fdr[0], &child_dr, sizeof(double)) < 0) {
                perror("read from pipe");
                exit(1);
        }

        // Find the smaller of two distances 
        double d = min(child_dl, child_dr);

        // Build an array strip[] that contains points close (closer than d) to the line passing through the middle point.
        struct Point *strip = malloc(sizeof(struct Point) * n);
        if (strip == NULL) {
            perror("malloc");
            exit(1);
        }

        int j = 0;
        for (int i = 0; i < n; i++) {
            if (abs(p[i].x - mid_point.x) < d) {
                strip[j] = p[i], j++;
            }
        }

        // Find the closest points in strip.  Return the minimum of d and closest distance in strip[].
        double new_min = min(d, strip_closest(strip, j, d));
        free(strip);

        *pcount += counter;

        return new_min;
}
