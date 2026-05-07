int get_temperature_celsius(int sensor_id) {
	if (sensor_id % 2 != 0) {
		return -1;
	}
	int temp = 20 + sensor_id % 15;
	return (temp < 20) ? 20 : (temp > 35 ? 35 : temp);
}

#ifdef __DEBUG_

#include <stdio.h>

int main(void) {
	for (int i = -3; i < 20; i++)
		printf("%d -> %d\n", i, get_temperature_celsius(i));
}
#endif