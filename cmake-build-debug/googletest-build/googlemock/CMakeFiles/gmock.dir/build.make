# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.29

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /opt/homebrew/Cellar/cmake/3.29.3/bin/cmake

# The command to remove a file.
RM = /opt/homebrew/Cellar/cmake/3.29.3/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug

# Include any dependencies generated for this target.
include googletest-build/googlemock/CMakeFiles/gmock.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include googletest-build/googlemock/CMakeFiles/gmock.dir/compiler_depend.make

# Include the progress variables for this target.
include googletest-build/googlemock/CMakeFiles/gmock.dir/progress.make

# Include the compile flags for this target's objects.
include googletest-build/googlemock/CMakeFiles/gmock.dir/flags.make

googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.o: googletest-build/googlemock/CMakeFiles/gmock.dir/flags.make
googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.o: /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googlemock/src/gmock-all.cc
googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.o: googletest-build/googlemock/CMakeFiles/gmock.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --progress-dir=/Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.o"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.o -MF CMakeFiles/gmock.dir/src/gmock-all.cc.o.d -o CMakeFiles/gmock.dir/src/gmock-all.cc.o -c /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googlemock/src/gmock-all.cc

googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Preprocessing CXX source to CMakeFiles/gmock.dir/src/gmock-all.cc.i"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googlemock/src/gmock-all.cc > CMakeFiles/gmock.dir/src/gmock-all.cc.i

googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Compiling CXX source to assembly CMakeFiles/gmock.dir/src/gmock-all.cc.s"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googlemock/src/gmock-all.cc -o CMakeFiles/gmock.dir/src/gmock-all.cc.s

# Object files for target gmock
gmock_OBJECTS = \
"CMakeFiles/gmock.dir/src/gmock-all.cc.o"

# External object files for target gmock
gmock_EXTERNAL_OBJECTS =

lib/libgmock.1.16.0.dylib: googletest-build/googlemock/CMakeFiles/gmock.dir/src/gmock-all.cc.o
lib/libgmock.1.16.0.dylib: googletest-build/googlemock/CMakeFiles/gmock.dir/build.make
lib/libgmock.1.16.0.dylib: lib/libgtest.1.16.0.dylib
lib/libgmock.1.16.0.dylib: googletest-build/googlemock/CMakeFiles/gmock.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --bold --progress-dir=/Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX shared library ../../lib/libgmock.dylib"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/gmock.dir/link.txt --verbose=$(VERBOSE)
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock && $(CMAKE_COMMAND) -E cmake_symlink_library ../../lib/libgmock.1.16.0.dylib ../../lib/libgmock.1.16.0.dylib ../../lib/libgmock.dylib

lib/libgmock.dylib: lib/libgmock.1.16.0.dylib
	@$(CMAKE_COMMAND) -E touch_nocreate lib/libgmock.dylib

# Rule to build all files generated by this target.
googletest-build/googlemock/CMakeFiles/gmock.dir/build: lib/libgmock.dylib
.PHONY : googletest-build/googlemock/CMakeFiles/gmock.dir/build

googletest-build/googlemock/CMakeFiles/gmock.dir/clean:
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock && $(CMAKE_COMMAND) -P CMakeFiles/gmock.dir/cmake_clean.cmake
.PHONY : googletest-build/googlemock/CMakeFiles/gmock.dir/clean

googletest-build/googlemock/CMakeFiles/gmock.dir/depend:
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googlemock /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googlemock/CMakeFiles/gmock.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : googletest-build/googlemock/CMakeFiles/gmock.dir/depend

