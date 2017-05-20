#include "lott.h"
#include "part.h"
#include <pthread.h>
#include <dirent.h>
#include <stdio.h>
#include <time.h>

static void* map(void*);
static void* reduce(void*);

    int NUM_THREADS= 0;
int part1(){
    DIR* dirp;
    DIR* dire;
    struct dirent* entry;
    struct dirent* enter;

    

    dire = opendir(DATA_DIR); 
    while ((enter = readdir(dire)) != NULL) {
        if (enter->d_type == DT_REG) { // If a regular file 
             NUM_THREADS++;
        }
    }closedir(dire);
    printf("%d threads found\n", NUM_THREADS);//returns 500
    struct site* data_array[NUM_THREADS];
    pthread_t threads[NUM_THREADS];
    int rc;
    int t;
    FILE *fp;

    dirp=opendir(DATA_DIR);
    for(t=-2; t<NUM_THREADS; t++){
      entry=readdir(dirp);
      if (entry->d_type ==DT_REG){
         struct site* website=malloc(sizeof(struct site));//make new struct for each site
         website->name = strdup(entry->d_name);           //start updating the info for the stcts
         website->thread_id = t;
         website->duration=0;
         char* slash = "/";
         char* dataorigin=malloc(21*sizeof(char));                     //make path
         strcpy(dataorigin,DATA_DIR);
         strcat(dataorigin,slash);
         char* datapath= malloc(40*sizeof(char));
         strcpy(datapath,dataorigin);
         strcat(datapath, website->name);                 //
         fp=fopen(datapath,"r");                          //use path
         if (fp == NULL)
            return -1;

 	       website->fd = fp;

         data_array[t]=website;
         rc = pthread_create(&threads[t], NULL, map, (void *)website);//make tthreads
         if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
         }
         printf("In main: created thread %d %s\n", t,website->name); 
         free(datapath);
         //free(website);
       }
    }
    closedir(dirp);
    printf(
        "Part: %s\n"
        "Query: %s\n",
        PART_STRINGS[current_part], QUERY_STRINGS[current_query]);

    for(t =0; t<NUM_THREADS;t++){
      pthread_join(threads[t],NULL);//join all threads
    }
    reduce((void*)data_array);

    for(t =0; t<NUM_THREADS;t++){
      free(data_array[t]);//free all structs 
    }

    return 0;
}

static void* map(void* v){


  struct site* v2=(struct site*)v;

         char line[100];
         char* saveptr;
         int lineCount=0;
	while (fgets(line,100,v2->fd)!=NULL){//for each line
        lineCount=lineCount+1;
        long unixtime = strtof(strtok_r(line,",",&saveptr),NULL);//calc yearsm part
      if((strcmp(QUERY_STRINGS[current_query],"C")==0)||(strcmp(QUERY_STRINGS[current_query],"D")==0)){
        struct tm *info;
        char *buffer = malloc(10);
        info = localtime(&unixtime);
        strftime(buffer, 10,"%Y", info);
        //printf("%s\n", buffer);
        char *ptr;
        int ret= strtol(buffer, &ptr, 10);
        ret -= 1970;
        v2->years[ret]+=1;
        
        free(buffer);
      }
        strtok_r(NULL,",",&saveptr);  //ipaddrses part

        v2->duration += strtof(strtok_r(NULL,",",&saveptr),NULL);//calc duration part
        
        char* cnty=strtok_r(NULL,",",&saveptr);  //calc country
      if((strcmp(QUERY_STRINGS[current_query],"E")==0)){
        int i;
        for(i=0;i<10;i++){
          if(v2->country[i]==NULL){
            v2->country[i]=strdup(cnty);
            v2->countryCount[i]+=1;
            break;
          }
          else if(strcmp(v2->country[i],cnty)==0){
            v2->countryCount[i]+=1;
            break;

          }
        }
      }
    }

  v2->duration=v2->duration/lineCount;//duration
  //printf("%d\n", lineCount);

  
  //printf("duration: %.5f\n",v2->duration);
  if((strcmp(QUERY_STRINGS[current_query],"E")==0)){//country
    int countyIndex;
    int countymax=0;
    int counter;
    for(counter=0;counter<10;counter++){
      if (v2->countryCount[counter]>countymax){

        countymax=v2->countryCount[counter];
        countyIndex=counter;

      }else if ((v2->countryCount[counter]==countymax)&&(strcmp(v2->country[countyIndex],v2->country[counter])>0)){
        countymax=v2->countryCount[counter];
        countyIndex=counter;
      }
    }
    //printf("%d in %s",v2->countryCount[countyIndex],v2->country[countyIndex]);
    v2->highestCountry=v2->country[countyIndex];
    v2->highestCountryCount=v2->countryCount[countyIndex];
  }
  int numActiveYears=0; float numTotalUsers=0;
  if(((strcmp(QUERY_STRINGS[current_query],"C")==0)||((strcmp(QUERY_STRINGS[current_query],"D")==0)))) {//years

    int startUnixYr=0;
    for(startUnixYr=0;startUnixYr<=68;startUnixYr++){
      if(v2->years[startUnixYr]>0){
        numActiveYears+=1;
        numTotalUsers+=v2->years[startUnixYr];
      }
    }
    //printf(" years: %d\n",numActiveYears);
    //printf("%s Avg Users: %.5f\n",v2->name,numTotalUsers/numActiveYears);
    v2->UserCount=numTotalUsers/numActiveYears;
  }

	fclose(v2->fd);  

  return v2;
}

static void* reduce(void* v){;
  struct site** v3 = (struct site**)v;

  if(strcmp(QUERY_STRINGS[current_query],"B")==0){
    int result=0;
    int minIndex;
    float min=v3[0]->duration;
    for(minIndex=1;minIndex<NUM_THREADS;minIndex++){
      if(v3[minIndex]->duration<min){
        min = v3[minIndex]->duration;
        result=minIndex;
      }
    }
    printf("Result: %.5f, %s\n",min, v3[result]->name );
  }else if(strcmp(QUERY_STRINGS[current_query],"A")==0){
    int result=0;
    int maxIndex;
    float max=v3[0]->duration;
    for(maxIndex=1;maxIndex<NUM_THREADS;maxIndex++){
      if(v3[maxIndex]->duration>max){
        max = v3[maxIndex]->duration;
        result=maxIndex;
      }
    }
    printf("Result: %.5f, %s\n",max, v3[result]->name );
  }else if(strcmp(QUERY_STRINGS[current_query],"C")==0){
    char* highestUserSite=v3[0]->name;
    float highestUser=v3[0]->UserCount;
    int k=1;
    for(k=1;k<NUM_THREADS;k++){
      if((v3[k]->UserCount>highestUser)){
        highestUserSite=v3[k]->name;
        highestUser=v3[k]->UserCount;
      }
      else if((v3[k]->UserCount==highestUser)&&(strcmp(v3[k]->name,highestUserSite)<0)){
        highestUserSite=v3[k]->name;
        highestUser=v3[k]->UserCount;
      }
    }
    printf("Result: %.5f, %s\n",highestUser, highestUserSite );
  }else if(strcmp(QUERY_STRINGS[current_query],"D")==0){

    char* highestUserSite=v3[0]->name;
    float highestUser=v3[0]->UserCount;
    int k=1;
    for(k=1;k<NUM_THREADS;k++){
      if((v3[k]->UserCount<highestUser)){
        highestUserSite=v3[k]->name;
        highestUser=v3[k]->UserCount;
      }else if((v3[k]->UserCount==highestUser)&&(strcmp(v3[k]->name,highestUserSite)<0)){
        highestUserSite=v3[k]->name;
      }
    }
    printf("Result: %.5f, %s\n",highestUser, highestUserSite );
    
  }else if(strcmp(QUERY_STRINGS[current_query],"E")==0){
    char* endArrayNames[10]={"","","","","","","","","",""};
    int endCount[10]={0};
    int i,x;
    for(i=0;i<NUM_THREADS;i++){
          //printf("%s : %d\n",v3[i]->name,v3[i]->highestCountryCount );

      for(x=0;x<10;x++){
        if(strcmp(v3[i]->highestCountry,endArrayNames[x])==0){
          //printf("hellos\n");
          endCount[x]+=v3[i]->highestCountryCount;
          break;

        }else if(strcmp(endArrayNames[x],"")==0){
          //printf("added\n");
          endCount[x]+=v3[i]->highestCountryCount;
          endArrayNames[x]=strdup(v3[i]->highestCountry);
          //printf("%s\n",endArrayNames[x] );
          break;
        }
      }

    }
    int b;
    int maxC=0;
    int maxCIndex;
    for(b=0;b<10;b++){
      if(endCount[b]>maxC){
        maxCIndex=b;
        maxC=endCount[b];
      }//printf("t: %d, %s\n",endCount[b],endArrayNames[b] );
    }
    printf("Result: %.5g, %s\n",(double)endCount[maxCIndex],endArrayNames[maxCIndex] );
  }
    return NULL;
}

