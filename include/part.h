#ifndef PART_H
#define PART_H

struct site{
  char* name;
  int thread_id;
  FILE * fd;
  int read;
  float years[68];//1970-2038
  float UserCount;

  float duration;
  
  char* country[10];
  float countryCount[10];
  char* highestCountry;
  float highestCountryCount;
};


#endif /* PART_H */
