sizeMb=60
fn="${sizeMb}MB_HDD.img"
dd if=/dev/zero of=$fn bs=1M count=$sizeMb
printf "g\nn\n1\n\nw\n" | fdisk $fn
mkfs.ext4 $fn
sudo mkdir /mnt/${sizeMb}MBdisk
sudo mount -o loop $fn /mnt/${sizeMb}MBdisk
sudo chmod ugo+wx /mnt/${sizeMb}MBdisk