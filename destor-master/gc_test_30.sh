

# !/bin/bash

# path="/home/liutao/backup_data/"
# for path1 in ${path}/*.8kb.hash.anon
# do
#     sudo ./destor "${path1}" 
#     sudo ./destor -g0
# done



# path="/home/liutao/restore_data/"

# for (( i = 0; i < 31; i++ )); do
#     sudo ./destor -g$i "${path}" 
# done



# count=0
# hundred=32
# path="/home/liutao/backup_data/"
# for path1 in ${path}/*.8kb.hash.anon
# do
#     sudo ./destor "${path1}" 
#     # sudo ./destor -g$count
#     if [ $count -eq $hundred ]
#     then
#         break
#     fi
#     let count+=1
# done
# exit

# count=0
# hundred=10
# path="/home/liutao/backup_data/"
# for path1 in ${path}/*.8kb.hash.anon
# do
#     sudo ./destor "${path1}" 
#     # sudo ./destor -g$count
#     if [ $count -eq $hundred ]
#     then
#         break
#     fi
#     let count+=1
# done
# exit



# hundred=31
# for (( i = 0; i < hundred; i++ )); do
# 	count=0
# 	path="/home/liutao/backup_data/"
# 	path2="/home/liutao/restore_data/"
# 	for path1 in ${path}/*.8kb.hash.anon
# 	do
# 	    sudo ./destor "${path1}" 
# 	    if [ $count -eq $i ]
# 	    then
# 	        break
# 	    fi
# 	    let count+=1
# 	done
# 	sudo ./destor -g0 "${path2}"
# 	sudo ./rebuild
# done

count=1
hundred=32
path="/home/liutao/backup_data/"
path2="/home/liutao/restore_data/"
for path1 in ${path}/*.8kb.hash.anon
do
    sudo ./destor "${path1}" 
    # sudo ./destor -g0
    if [ $count -eq $hundred ]
    then
        break
    fi
    let count+=1
done
sudo ./destor -g0 "${path2}"
sudo ./destor -g1 "${path2}"
sudo ./destor -g2 "${path2}"
sudo ./destor -g3 "${path2}"
sudo ./destor -g4 "${path2}"
sudo ./destor -g5 "${path2}"
sudo ./destor -g6 "${path2}"
sudo ./destor -g7 "${path2}"
sudo ./destor -g8 "${path2}"
sudo ./destor -g9 "${path2}"
sudo ./destor -g10 "${path2}"
sudo ./destor -g11 "${path2}"
sudo ./destor -g12 "${path2}"
sudo ./destor -g13 "${path2}"
sudo ./destor -g14 "${path2}"
sudo ./destor -g15 "${path2}"
exit


