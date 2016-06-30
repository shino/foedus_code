First touch of FOEDUS/libfoedus-core

# Purpose

Run the example in FOEDUS README file.

References

- hkimura/foedus_code: FOEDUS main source code repository
  - https://github.com/hkimura/foedus_code
  - `git clone git@github.com:hkimura/foedus_code.git`
  - commit `f71ce60d6df671e6f0b3ac90fdcdf751aebd7a05`
- libfoedus-core: Main Page
  http://cihead.labs.hpe.com/centos7/job/foedus-master-doxygen/doxygen/

# Environment (of me)

```
$ uname -a
Linux localhost.localdomain 4.2.3-300.fc23.x86_64 #1 SMP Mon Oct 5 15:42:54 UTC 2015 x86_64 x86_64 x86_64 GNU/Linux
$ cat /etc/redhat-release
Fedora release 23 (Twenty Three)
```

Install dependent packages

```
sudo yum -y install gcc gcc-c++ libstdc* cmake glibc glibc-* valgrind valgrind-devel
sudo yum -y install libunwind libunwind-devel libdwarf libdwarf-devel
sudo yum -y install numactl numactl-devel google-perftools google-perftools-devel
sudo yum -y install papi papi-devel papi-static
sudo yum -y install python python-*
sudo yum -y install doxygen texlive-eps* graphviz mscgen texlive-epspdf sloccount kdevelop cloc
```

Some version information

```
$ gcc --version
gcc (GCC) 5.3.1 20160406 (Red Hat 5.3.1-6)
Copyright (C) 2015 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

$ cmake --version
cmake version 3.4.3

CMake suite maintained and supported by Kitware (kitware.com/cmake).

$ dnf info installed glibc | grep Version
Version     : 2.22

$ valgrind --version
valgrind-3.11.0

$ papi_version
PAPI Version: 5.4.1.0
```

Kernel settings, ad-hoc way

```
sudo sh -c 'echo 2048 > /proc/sys/vm/nr_hugepages'

sudo sysctl -w kernel.shmmax=9223372036854775807
sudo sysctl -w kernel.shmall=1152921504606846720
sudo sysctl -w kernel.shmmni=409600
sudo sysctl -w vm.max_map_count=2147483647
sudo sysctl -p
```

Kernel settings, permanent way

`/etc/sysctl.conf`

```
kernel.shmmax = 9223372036854775807
kernel.shmall = 1152921504606846720
kernel.shmmni = 409600
vm.max_map_count = 2147483647
vm.nr_hugepages = 2048
```

`/etc/security/limits.conf`

```
shino  - nofile 655360
shino  - nproc 655360
shino  - rtprio 99
shino  - memlock -1
```

Then, Reboot!

# Build foedus, including foedus-core

```
$ cd <cloned-directory>
$ mkdir build
$ cd build
$ cmake ..
$ make [-j <concurrency>]
```

# Run Test

`ctest`, or `ctest -R '^test_check_env_Hugepages'` for the impatient.


# Run the first example

Example code is in `foedus-core/README.markdown`.

The example code is in:
https://github.com/shino/foedus_code/tree/tmp/first-example/examples/first , which
assumes `libfoedus-core` has already been built under `foedus_code/build/` directory.

Build and Run:

```
$ cd <cloned directory>
$ mkdir examples/first/build
$ cd examples/first/build
$ cmake ..
$ make
$ ./first
```
