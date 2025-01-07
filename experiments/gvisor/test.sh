set -e

ROOT=$(pwd)

check_gvisor() {
  # Check if we have runsc in the system
  if [ -x "$(command -v runsc)" ]; then
      echo "runsc is already installed"
      return
  fi
  echo "Gvisor not installed"
  exit 1
}

build_rootfs() {
  if [ ! -d "rootfs" ]; then 
		wget -q https://dl-cdn.alpinelinux.org/alpine/v3.18/releases/x86_64/alpine-minirootfs-3.18.4-x86_64.tar.gz; \
		mkdir -p rootfs; \
		tar -xzf alpine-minirootfs-3.18.4-x86_64.tar.gz -C rootfs; \
		rm -f alpine-minirootfs-3.18.4-x86_64.tar.gz; \
    cp /etc/resolv.conf rootfs/etc/resolv.conf # Ensure DNS works in chroot
    sudo chroot rootfs /bin/sh -c "apk update && apk add --no-cache python3"; \
	fi
}

run_gvisor() {
  echo "Running gvisor in " $(pwd)
  sudo runc run test-container
}

copy_config() {
  config_file=$1
  if [ -z "$config_file" ]; then
    config_file="config.json"
  fi

  sed "s|\$CWD|$(pwd)|g" $ROOT/$config_file > config.json
}

test_on_host() {
  cd $ROOT

  rm -rf test/host_tests
  mkdir -p test/host_tests
  cd test/host_tests
  cp $ROOT/engine.py .

  touch updated.txt

  rows=$1
  mode=$2
  if [ -z "$rows" ]; then
    rows=1000
  fi
  if [ -z "$mode" ]; then
    mode="normal"
  fi


  python3 engine.py ./file.db $rows $mode
}

test_under_gvisor() {
  cd $ROOT

  check_gvisor
  build_rootfs
  sudo rm -rf test/gvisor_tests
  mkdir -p test/gvisor_tests
  cd test/gvisor_tests
  cp -R $ROOT/rootfs .
  copy_config
  mkdir data
  cp $ROOT/file.db data/file.db
  touch data/updated.txt

  # Allow recursive permission 777 to data
  sudo chmod -R 777 data

  mkdir app
  cp $ROOT/engine.py app

  rows=$1
  mode=$2
  if [ -z "$rows" ]; then
    rows=1000
  fi
  if [ -z "$mode" ]; then
    mode="normal"
  fi


  build_run_sh $rows $mode
  run_gvisor
}

test_under_gvisor_overlay() {
  cd $ROOT

  check_gvisor
  build_rootfs
  sudo rm -rf test/gvisor_overlay
  mkdir -p test/gvisor_overlay
  cd test/gvisor_overlay
  cp -R $ROOT/rootfs .
  echo "Using overlayfs"
  copy_config config_overlay.json
  mkdir data
  mkdir data/upper
  mkdir data/work
  mkdir data/lower
  touch data/lower/updated.txt

  # echo "Copying file.db to data/file.db"
  cp $ROOT/file.db data/lower/file.db

  # Allow recursive permission 777 to data
  sudo chmod -R 777 data

  mkdir app
  cp $ROOT/engine.py app

  rows=$1
  mode=$2
  if [ -z "$rows" ]; then
    rows=1000
  fi
  if [ -z "$mode" ]; then
    mode="normal"
  fi


  build_run_sh $rows $mode
  run_gvisor
}


build_run_sh() {
  # Read number of rows from arguments
  rows=$1
  mode=$2

  # Provide default parameters, 1000 and normal
  if [ -z "$rows" ]; then
    rows=1000
  fi
  if [ -z "$mode" ]; then
    mode="normal"
  fi

  echo "python3 engine.py /sandbox/data/file.db $rows $mode" > app/run.sh
}

sudo rm -rf test

MAX_ROWS=5000

echo "############## Testing JOURNAL MODE under gvisor with overlay ##############"
test_under_gvisor_overlay $MAX_ROWS normal

echo "############## Testing JOURNAL MODE under gvisor ##############"
test_under_gvisor $MAX_ROWS normal

echo "############## Testing JOURNAL MODE on host ##############"
test_on_host $MAX_ROWS normal



echo "############## Testing WAL MODE under gvisor with overlay ##############"
test_under_gvisor_overlay $MAX_ROWS wal

echo "############## Testing WAL MODE under gvisor ##############"
test_under_gvisor $MAX_ROWS wal

echo "############## Testing WAL MODE on host ##############"
test_on_host $MAX_ROWS wal

