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
include googletest-build/googletest/CMakeFiles/gtest_main.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include googletest-build/googletest/CMakeFiles/gtest_main.dir/compiler_depend.make

# Include the progress variables for this target.
include googletest-build/googletest/CMakeFiles/gtest_main.dir/progress.make

# Include the compile flags for this target's objects.
include googletest-build/googletest/CMakeFiles/gtest_main.dir/flags.make

googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.o: googletest-build/googletest/CMakeFiles/gtest_main.dir/flags.make
googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.o: /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googletest/src/gtest_main.cc
googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.o: googletest-build/googletest/CMakeFiles/gtest_main.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --progress-dir=/Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.o"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.o -MF CMakeFiles/gtest_main.dir/src/gtest_main.cc.o.d -o CMakeFiles/gtest_main.dir/src/gtest_main.cc.o -c /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googletest/src/gtest_main.cc

googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Preprocessing CXX source to CMakeFiles/gtest_main.dir/src/gtest_main.cc.i"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googletest/src/gtest_main.cc > CMakeFiles/gtest_main.dir/src/gtest_main.cc.i

googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Compiling CXX source to assembly CMakeFiles/gtest_main.dir/src/gtest_main.cc.s"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googletest/src/gtest_main.cc -o CMakeFiles/gtest_main.dir/src/gtest_main.cc.s

# Object files for target gtest_main
gtest_main_OBJECTS = \
"CMakeFiles/gtest_main.dir/src/gtest_main.cc.o"

# External object files for target gtest_main
gtest_main_EXTERNAL_OBJECTS =

lib/libgtest_main.1.16.0.dylib: googletest-build/googletest/CMakeFiles/gtest_main.dir/src/gtest_main.cc.o
lib/libgtest_main.1.16.0.dylib: googletest-build/googletest/CMakeFiles/gtest_main.dir/build.make
lib/libgtest_main.1.16.0.dylib: lib/libgtest.1.16.0.dylib
lib/libgtest_main.1.16.0.dylib: googletest-build/googletest/CMakeFiles/gtest_main.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --bold --progress-dir=/Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX shared library ../../lib/libgtest_main.dylib"
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/gtest_main.dir/link.txt --verbose=$(VERBOSE)
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest && $(CMAKE_COMMAND) -E cmake_symlink_library ../../lib/libgtest_main.1.16.0.dylib ../../lib/libgtest_main.1.16.0.dylib ../../lib/libgtest_main.dylib

lib/libgtest_main.dylib: lib/libgtest_main.1.16.0.dylib
	@$(CMAKE_COMMAND) -E touch_nocreate lib/libgtest_main.dylib

# Rule to build all files generated by this target.
googletest-build/googletest/CMakeFiles/gtest_main.dir/build: lib/libgtest_main.dylib
.PHONY : googletest-build/googletest/CMakeFiles/gtest_main.dir/build

googletest-build/googletest/CMakeFiles/gtest_main.dir/clean:
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest && $(CMAKE_COMMAND) -P CMakeFiles/gtest_main.dir/cmake_clean.cmake
.PHONY : googletest-build/googletest/CMakeFiles/gtest_main.dir/clean

googletest-build/googletest/CMakeFiles/gtest_main.dir/depend:
	cd /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/thirdparty/googletest/googletest /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest /Users/zhanyuxiao/Desktop/Class_materials/database/labs/minisql/cmake-build-debug/googletest-build/googletest/CMakeFiles/gtest_main.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : googletest-build/googletest/CMakeFiles/gtest_main.dir/depend

