# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.12

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /home/parallels/Downloads/clion-2018.2.5/bin/cmake/linux/bin/cmake

# The command to remove a file.
RM = /home/parallels/Downloads/clion-2018.2.5/bin/cmake/linux/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/parallels/Desktop/engine/engine_race

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/parallels/Desktop/engine/engine_race/cmake-build-debug

# Include any dependencies generated for this target.
include CMakeFiles/engine_race.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/engine_race.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/engine_race.dir/flags.make

CMakeFiles/engine_race.dir/engine_race.cc.o: CMakeFiles/engine_race.dir/flags.make
CMakeFiles/engine_race.dir/engine_race.cc.o: ../engine_race.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/parallels/Desktop/engine/engine_race/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/engine_race.dir/engine_race.cc.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/engine_race.dir/engine_race.cc.o -c /home/parallels/Desktop/engine/engine_race/engine_race.cc

CMakeFiles/engine_race.dir/engine_race.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/engine_race.dir/engine_race.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/parallels/Desktop/engine/engine_race/engine_race.cc > CMakeFiles/engine_race.dir/engine_race.cc.i

CMakeFiles/engine_race.dir/engine_race.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/engine_race.dir/engine_race.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/parallels/Desktop/engine/engine_race/engine_race.cc -o CMakeFiles/engine_race.dir/engine_race.cc.s

# Object files for target engine_race
engine_race_OBJECTS = \
"CMakeFiles/engine_race.dir/engine_race.cc.o"

# External object files for target engine_race
engine_race_EXTERNAL_OBJECTS =

engine_race: CMakeFiles/engine_race.dir/engine_race.cc.o
engine_race: CMakeFiles/engine_race.dir/build.make
engine_race: CMakeFiles/engine_race.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/parallels/Desktop/engine/engine_race/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable engine_race"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/engine_race.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/engine_race.dir/build: engine_race

.PHONY : CMakeFiles/engine_race.dir/build

CMakeFiles/engine_race.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/engine_race.dir/cmake_clean.cmake
.PHONY : CMakeFiles/engine_race.dir/clean

CMakeFiles/engine_race.dir/depend:
	cd /home/parallels/Desktop/engine/engine_race/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/parallels/Desktop/engine/engine_race /home/parallels/Desktop/engine/engine_race /home/parallels/Desktop/engine/engine_race/cmake-build-debug /home/parallels/Desktop/engine/engine_race/cmake-build-debug /home/parallels/Desktop/engine/engine_race/cmake-build-debug/CMakeFiles/engine_race.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/engine_race.dir/depend

