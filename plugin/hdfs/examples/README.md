The example codes are to demonstrate how to use run any simple example but in the hdfs environment. I will slowly add more examples to this list to expand it further, but for the time being a simple applicaiton is testhdfs.cc

# Build and Run
Make sure that you have completed the build process for hdfs in the parent folder's README (the final make command is not required if you are using CMake). I usually create a seperate folder in the rocksdb root directory called scripts to run any scripts that I write and for the time being, this guide will follow the same.

## With Makefile
The current prodcess for make is not as streamlined as I would like it to be and I will update it soon. 

Assuming you are in the rocksdb root folder
```
mkdir scripts
mv ./plugin/hdfs/examples/* ./scripts
cd scripts
```
Perform the make and you can simply run the file
```
make testhdfs
./testhdfs
```

## With CMake
Assuming you are in the rocksdb root folder. Add the following lines at the end of the CMakeLists.txt file (keeping build with scripts on by default, will update in the future)
```
option(WITH_SCRIPTS "build with scripts folder" ON)
if(WITH_SCRIPTS)
  add_subdirectory(scripts)
endif()
```
Craete thge build folder and add the cmake files
```
mkdir build
cd build
cmake .. -DROCKSDB_PLUGINS="hdfs"
```
Perform the make and you can simply run the file
```
make -j48 testhdfs
./scrips/testhdfs
```
