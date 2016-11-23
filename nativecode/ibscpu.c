/**
1 microsecond = 1.0*10-6 seconds
gettimeofday
**/
#include <stdio.h>
#include <sys/time.h>
int main (int argc,char *argv[]) {
	int i, j, l, k, m, jj;
        jj = 2342;
        k = 31455;
        l = 16452;
        m = 9823;
        i = 86;
        char* loops = argv[1];
        unsigned long long count = atoi(loops);
        printf("loops: %lld\n", count);
        // count time in microseconds
        struct timeval stop, start;
        gettimeofday(&start, NULL);
        printf("start:%u\n", start);
        while (count--) {
                m       = m ^ l; k = (k / m * jj) % i; l = j * m * k;
                i       = (j * k) ^ m; k = (k / m * jj) % i; m = m ^ l;
                m       = m ^ l; i = (j * k) ^ m; k = (k / m * jj) % i;
                m       = i * i * i * i * i * i * i; // m=k*l*jj*l; 
                m       = m ^ l; k = (k / m * jj) % i; l = j * m * k; i = (j * k) ^ m;
                l       = (k / m * jj) % i; m = m ^ l; m = m ^ l; i = (j * k) ^ m;
                k       = (k / m * jj) % i; m = k * k * k * k * k - m / i;
        }
        gettimeofday(&stop, NULL);
        printf("stop:%u\n", stop);
       // printf("duration: %lu\n", stop.tv_usec - start.tv_usec);
	return 0;
}



