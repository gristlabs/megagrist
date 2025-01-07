	if [ ! -d "rootfs" ]; then 
		wget -q https://dl-cdn.alpinelinux.org/alpine/v3.18/releases/x86_64/alpine-minirootfs-3.18.4-x86_64.tar.gz; \
		mkdir -p rootfs; \
		tar -xzf alpine-minirootfs-3.18.4-x86_64.tar.gz -C rootfs; \
		rm -f alpine-minirootfs-3.18.4-x86_64.tar.gz; \
	fi