# cache and bloom filter based on midterm

## How to switch among HRW, CH with Virtual Node and Naive Hash:
![Image text](https://github.com/iloveleejunghyun/cmpe273-assignment4/blob/master/switch.png)  
In cache_client.py, you can see there are 3 lines(14~16) to switch among naive hash, RHW hash and Virtual Node Consistent hash.

## How to open data replication function in Consistent-Hash with Virtual Node:
![Image text](https://github.com/iloveleejunghyun/cmpe273-assignment4/blob/master/replication.bmp)  
In cache_client.py, you can see there are 3 lines(51~53) to open data replication in Virtual Node Consistent hash.
I use a single method to realize replication, which puts backup data on the next physical node of the original node. 


# Consistent Hashing and RHW Hashing

The distributed cache you implemented in the midterm is based on naive modula hashing to shard the data.

## Part I.

Implement Rendezvous hashing to shard the data.


## Part II.

Implement consistent hashing to shard the data.

Features:

* Add virtual node layer in the consistent hashing.
* Implement virtual node with data replication. 