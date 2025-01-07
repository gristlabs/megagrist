## Gvisor file system experiments.

This folder contains the experiments with accessing and modifying the file system from within a gvisor process.

## Experiments


### SQLite mode

File being accessed and written is a sqlite database file. Here we are testing two modes:
1. Default journal mode.
https://www.sqlite.org/lockingv3.html#rollback

2. WAL mode.
https://www.sqlite.org/wal.html#overview

The WAL mode offers much better performance than the default journal mode in general.

### Transactions

For each of the mode we perform two tests:
1. Inserting (and reading) 5000 rows, each one in a separate transaction.
2. Inserting (and reading) 5000 rows, all in a single transaction.


### Gvisor filesystem access

File is access from a python script using the sqlite3 library. This python script is run in 3 different ways:

1. Directly on the host.

2. Sandboxed under gvisor runsc with direct fs access.
https://gvisor.dev/docs/user_guide/filesystem/#directfs

This is 'new' way of accessing the file system in gvisor. Before gvisor was using a proxy (gofer) to access the file system
for every system call. This was slow and inefficient. The direct fs access is much faster, as the gofer is only
used for accessing the files and creating file descriptors. Then the sandbox makes direct call using the file descriptors.

3. Sandboxed under gvisor runsc with an overlay system.
https://gvisor.dev/docs/user_guide/filesystem/#sandbox-overlay
https://gvisor.dev/blog/2023/05/08/rootfs-overlay/

This is using an overlay file system. File is ready from the host but the changes are written to a separate file system.


### Running the experiments

In order to run you need to have runsc installed on your machine. You can follow the instructions here:
https://gvisor.dev/docs/user_guide/install/#install-from-an-apt-repository
or just run the following command:
```bash
bash install.sh
```

Experiments where run on a Ubuntu 20.04 machine. To run simple run the following command:

```bash
bash test.sh
```

### Results

| SQL Mode | Environment           | Elapsed Time for N transactions (seconds) | Elapsed Time for bulk in 1 transaction (seconds) |
|----------|-----------------------|-------------------------------------------|--------------------------------------------------|
| Journal  | gvisor with overlay   | 28.15                                     | 2.02                                             |
| Journal  | gvisor                | 28.36                                     | 2.05                                             |
| Journal  | host                  | 27.03                                     | 1.25                                             |
| WAL      | gvisor with overlay   | 9.45                                      | 2.00                                             |
| WAL      | gvisor                | 9.43                                      | 1.98                                             |
| WAL      | host                  | 8.17                                      | 1.22                                             |


From the result we can see, that GVisor adds some overhead but it is not significant.