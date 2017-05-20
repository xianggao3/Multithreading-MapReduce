#include "lott.h"
#include "part.h"
#include <pthread.h>
#include <dirent.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
//#include <semaphore.h>
#include <math.h>
static void* map(void*);
void* multimap3(void*);
char* maximumName;
float maximumYear;
int done;

static void* reduce(void*);
    
    int NUM_FILES3= 0;
    int numperthread3;
    struct site** data_array3;
    pthread_mutex_t resourceLock,readerLock, priorityLock;


    FILE *tempw;
    FILE *tempr;

int part3(size_t nthreads){
    tempw=fopen("mapred.tmp","w");
    tempr=fopen("mapred.tmp","r");

    DIR* dirp;
    DIR* dire;
    struct dirent* entry;
    struct dirent* enter;


    dire = opendir(DATA_DIR); 
    while ((enter = readdir(dire)) != NULL) {
        if (enter->d_type == DT_REG) { // If a regular file 
             NUM_FILES3++;
        }
    }closedir(dire);
    //printf("%d threads found\n", NUM_FILES3);//returns 500
    data_array3=malloc((NUM_FILES3)*sizeof(struct site*));
    if(NUM_FILES3<nthreads){
        nthreads=NUM_FILES3;
    }
    if(nthreads==0){
        printf("Error: 0 threads created.\n");
        return -1;
    }
    for(int d=0; d<NUM_FILES3;d++){
        data_array3[d]=NULL;
    }
    pthread_t threads[nthreads+1];
    int rc;
    int t;
    FILE *fp;

    //printf("%d\n", NUM_FILES3);

    dirp=opendir(DATA_DIR);
    for(t=-2; t<NUM_FILES3; t++){
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

         data_array3[t]=website;
         free(dataorigin);
         free(datapath);
         //free(website);
       }//
    }

    int tc;

    numperthread3=NUM_FILES3/nthreads;
    //int numperthrea=numperthread3;
    if ((NUM_FILES3%nthreads)!=0){
        numperthread3++;
    }
    //printf("%d\n",NUM_FILES3  );

    if((strcmp(QUERY_STRINGS[current_query],"D")==0)||(strcmp(QUERY_STRINGS[current_query],"B")==0) ){
      maximumYear=999999;
    }else{
      maximumYear=0;
    }


    pthread_create(&threads[nthreads],NULL,reduce,(void*)tempr);//reduce

    for(tc=0;tc<nthreads;tc++){
        //printf("%d\n",tc );
        int* baseIndex = malloc(sizeof(int));
        *baseIndex = tc*numperthread3;
        rc = pthread_create(&threads[tc], NULL, multimap3, (void*)baseIndex);//make tthreads
         if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
         }
         //printf("In main: created thread %d \n", tc); 
         //free(baseIndex);

    }
    closedir(dirp);
    printf(
        "Part: %s\n"
        "Query: %s\n",
        PART_STRINGS[current_part], QUERY_STRINGS[current_query]);

    for(t =0; t<nthreads;t++){
      pthread_join(threads[t],NULL);//join all threads
    }
    done=0;
    //pthread_cancel(threads[nthreads]);
    pthread_join(threads[nthreads],NULL);
    //while(1);
    //pthread_exit(NULL);
    if((strcmp(QUERY_STRINGS[current_query],"E")==0)){
            printf("Result:%.5g, %s\n", (double)maximumYear,maximumName);
    }else{
      printf("Result:%.5f, %s\n", maximumYear,maximumName);
    }
    for(t =0; t<NUM_FILES3;t++){
      free(data_array3[t]);//free all structs 
    }
    free(data_array3);
    fclose(tempw);
    fclose(tempr);
    unlink("mapred.tmp");
    return 0;
}

 void* multimap3(void* base){
    //printf("%d\n",numperthread3 );
    int* da = (int *) base;
    int nUntilNum;
    for(nUntilNum=0;nUntilNum<numperthread3;nUntilNum++){
        //printf("%d\n",nUntilNum );
        if(*da+nUntilNum<NUM_FILES3){
          //printf("Multimapping file %d\n",*da+nUntilNum);
            map((void*)data_array3[*da+nUntilNum]);
        }//else{
        //    printf("FOUND NULL \n");
        //}
    } 
    return da;
}

static void* map(void* v){
    //fflush(tempw);
  struct site* v2=(struct site*)v;
        //printf("%s\n",v2->name );
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
    //printf("Country: %s has %d users.\n",v2->country[countyIndex],v2->countryCount[countyIndex]);
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

  if(((strcmp(QUERY_STRINGS[current_query],"C")==0)||((strcmp(QUERY_STRINGS[current_query],"D")==0)))) {//WRITE TO TEMPFILE DIFFERENT INPUT DEPENDING ON WHAT QUERY USED
    pthread_mutex_lock(&resourceLock);  
    fprintf(tempw, "%.5f,%s\n",v2->UserCount,v2->name );
    fflush(tempw);
    //printf("%5f,%s,\n",v2->UserCount,v2->name);
    pthread_mutex_unlock(&resourceLock);
  }else if(((strcmp(QUERY_STRINGS[current_query],"A")==0)||((strcmp(QUERY_STRINGS[current_query],"B")==0)))) {
    pthread_mutex_lock(&resourceLock);
    fprintf(tempw, "%.5f,%s\n",v2->duration,v2->name );
    fflush(tempw);
    //printf("%.5f,%s,\n",v2->duration,v2->name );
    pthread_mutex_unlock(&resourceLock);
  }else if (strcmp(QUERY_STRINGS[current_query],"E")==0){
    pthread_mutex_lock(&resourceLock);
    fprintf(tempw, "%f,%s",v2->highestCountryCount,v2->highestCountry );
    fflush(tempw);

    pthread_mutex_unlock(&resourceLock);                                  //FINISH WRITE N CLOSE FD
  }

    fclose(v2->fd);  
    //fflush(tempw);
  return v2;
}

static void* reduce(void* v){
//  struct site** v3 = (struct site**)v;
    char* allCountries[10];
          float allCount[10];
              for(int i=0;i<10;i++){
                allCountries[i]="";
                allCount[i]=0;
              }
    int cock=0;
    done=1;
    while(done){
        usleep(1000);

        //printf("p");
    char line[100];
    char* saveptr;
    pthread_mutex_lock(&readerLock);
    pthread_mutex_lock(&resourceLock);
    pthread_mutex_unlock(&readerLock);

        //puts(line);
        //printf("LINE: %s\n", line);
        if(strcmp(QUERY_STRINGS[current_query],"B")==0){      //QUERY B READING
          while(fgets(line,100,tempr)!=NULL){
              float avgYear= strtof(strtok_r(line,",",&saveptr),NULL);
              char* nameOfSite=strtok_r(NULL,",",&saveptr);
              if(avgYear<maximumYear){

                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);

              }else if((avgYear==maximumYear)&&(strcmp(nameOfSite,maximumName)<0)){
                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);
              }
          }
          //printf("Result:  %d %.5f, %s\n",cock, maximumYear,maximumName); //DONT MIND VAR NAMES, LAZY so i copied and pasted

        }else if(strcmp(QUERY_STRINGS[current_query],"A")==0){
          while(fgets(line,100,tempr)!=NULL){
              float avgYear= strtof(strtok_r(line,",",&saveptr),NULL);
              char* nameOfSite=strtok_r(NULL,",",&saveptr);
              if(avgYear>maximumYear){
                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);

              }else if((avgYear==maximumYear)&&(strcmp(nameOfSite,maximumName)<0)){
                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);
              }
          }
          //if(fin==1){
            //printf("Result:  %d %.5f, %s\n",cock, maximumYear,maximumName);
          //  return 0;
          //}
        }else if(strcmp(QUERY_STRINGS[current_query],"C")==0){            //DONT MIND VAR NAMES, LAZY so i copied and pasted
          while(fgets(line,100,tempr)!=NULL){
              float avgYear= strtof(strtok_r(line,",",&saveptr),NULL);
              char* nameOfSite=strtok_r(NULL,",",&saveptr);
              if(avgYear>maximumYear){

                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);

              }else if((avgYear==maximumYear)&&(strcmp(nameOfSite,maximumName)<0)){
                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);
              }
          }
        }else if(strcmp(QUERY_STRINGS[current_query],"D")==0){            //DONT MIND VAR NAMES, LAZY so i copied and pasted
          while(fgets(line,100,tempr)!=NULL){
              //printf("A");
              cock++;
              float avgYear= strtof(strtok_r(line,",",&saveptr),NULL);
              char* nameOfSite=strtok_r(NULL,",",&saveptr);
              if(avgYear<maximumYear){

                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);

              }else if((avgYear==maximumYear)&&(strcmp(nameOfSite,maximumName)<0)){
                maximumYear=avgYear;
                maximumName=strdup(nameOfSite);
              }
          }
          //printf("Result:  %d %.5f, %s\n",cock, maximumYear,maximumName);

        }else if(strcmp(QUERY_STRINGS[current_query],"E")==0){          //DONT MIND VAR NAMES, LAZY so i copied and pasted
          

          //printf("A");
          while(fgets(line,100,tempr)!=NULL){                               //parse thru tempfile storing the avg users and the country codes into the array
              float avgYear= strtof(strtok_r(line,",",&saveptr),NULL);
              char* nameOfSite=strtok_r(NULL,",",&saveptr);

              for(int i=0;i<10;i++){      
                if(strcmp(allCountries[i],"")==0){
                  allCountries[i]=strdup(nameOfSite);
                  allCount[i]+=avgYear;
                  break;

                }
              else if(strcmp(allCountries[i],nameOfSite)==0){
                allCount[i]+=avgYear;
                break;
                }
                
              }

              
          }
        }

    if (strcmp(QUERY_STRINGS[current_query],"E")==0){             //sum up the avg users per year and find biggest
      for(int i=0;i<10;i++){  
        //printf("%s : %f\n",allCountries[i],allCount[i] );
          if(allCount[i]!=0){
            if(allCount[i]>maximumYear){
              maximumYear=allCount[i];
              maximumName=strdup(allCountries[i]);

            }else if((allCount[i]==maximumYear)&&(strcmp(allCountries[i],maximumName)<0)){
              maximumYear=allCount[i];
              maximumName=strdup(allCountries[i]);
            }
          }
        }
    }
    
    pthread_mutex_lock(&readerLock);
    pthread_mutex_unlock(&resourceLock);
    pthread_mutex_unlock(&readerLock);
    }

    return 0;//make seperate array and free stuff
}

