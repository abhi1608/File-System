/*
 *  Copyright (C) 2022 CS416/518 Rutgers CS
 *	RU File System
 *	File:	rufs.c
 *
 */

#define FUSE_USE_VERSION 26
#define F 0
#define D 1
#define RW 0
#define RWX 1

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>
#include <pthread.h>

#include "block.h"
#include "rufs.h"

pthread_mutex_t mutex;

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here

//Stores the actual super block struct information for reference
struct superblock sb;

//This is the pointer to block 0 (block containing superblock info) of virtual disk
void * sb_ptr;

bitmap_t inode_bit_map;
bitmap_t data_bit_map;

//This is the number of inodes that fit into a single block of the inode region
int num_inodes_per_block = BLOCK_SIZE/sizeof(struct inode);

//This is the number of dirents that fit into a single block of the data region
int num_dirents_per_block = BLOCK_SIZE/sizeof(struct dirent);

//Helper functions
void format_block_to_dirents (int block_num);

void write_bit_map(char c);

int name_exist(struct inode presentDirectory, const char * fname);



 //TODO: Rename the global variables

// TODO: Change the name of all once whole coding is done
// This function will format the block, this block is filled with
// array of directory entries. Once the block is intialized with directory entries
// it is copied back to disk using bio_write.
void format_block_to_dirents (int block_num)
{

	// Allocate Memory for array of directories that are to be copied into the block number
	struct dirent * arr = (struct dirent *)malloc(num_dirents_per_block * sizeof(struct dirent));
	
	// Intialize the array with 0.
	memset(arr, '\0', num_dirents_per_block * sizeof(struct dirent));

	for (int i = 0; i < num_dirents_per_block; i++)
	{
		arr[i].valid = 0;
	}

	// Create a temporary buffer which is used to copy back to disk
	void * buffer = malloc(BLOCK_SIZE);
	memset(buffer, '\0', BLOCK_SIZE);

	// Copy the array of directory entries to buffer
	memcpy(buffer, (void *)arr, num_dirents_per_block * sizeof(struct dirent));
	
	// Write the buffer into the disk
	bio_write(block_num, buffer);

	//TODO : free(buffer);

	free(arr);
}

//This function writes bitmaps to the disk
void write_bit_map(char c)
{
	//Allocating the memory equal to size of block
	void * buffer = malloc(BLOCK_SIZE);
	if(c == 'i')
	{
		// Copy the inode bitmap which is of size(MAX_INUM/8) to the buffer
    	memcpy(buffer, (const void *) inode_bit_map, (MAX_INUM)/8);
		// Write it to the disk
    	bio_write(sb.i_bitmap_blk, (const void *)buffer);
	}
	else
	{
		//Copy the data bitmap to buffer and the write it to disk
		memcpy(buffer, (const void *) data_bit_map, (MAX_DNUM)/8);
		// Write it to the disk
		bio_write(sb.d_bitmap_blk, (const void *)buffer);
	}
	// Free the buffer once the bitmap is written to the disk
    free(buffer);
}


//returns 0 if the file already exists, else returns 1
// This helper function takes the inode of directory that is
// of interest and seqarches whether the given file is present in 
// this directory.
int name_exist(struct inode dir, const char * fname)
{
	int found = 1;

	int num_data_blocks = dir.size;

	// Iterate through the data blocks
	for(int i = 0; i < num_data_blocks; i++)
	{
		//Getting the block pointed by direct pointer
		int data_block = dir.direct_ptr[i];

		// Allocate the buffer
		void * buffer = malloc(BLOCK_SIZE);
		// Read the corresponding data block from disk and store it in buffer
		bio_read(data_block, buffer);
		
		struct dirent * p = buffer;
		
		for (int j = 0; j < num_dirents_per_block; j++)
		{

			// Check if the directory is valid
			if((p+j) -> valid == 1)
			{
				char * cur_name = (p+j) -> name;
				// Check if file name passed matches with any file present in directory
				if (strcmp(cur_name, fname) == 0)
				{
					// Found the file
					found = 0;
					// Condition so we break from the for loops
					j = num_dirents_per_block+1;
					i = num_data_blocks + 1;
				}
			}
		}
		free(buffer);
	}
	return found;
}


/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {

	// Step 1: Read inode bitmap from disk
	
	// Step 2: Traverse inode bitmap to find an available slot

	// Step 3: Update inode bitmap and write to disk 

	for(int i = 0; i < MAX_INUM; i++)
	{
		uint8_t bit = get_bitmap(inode_bit_map, i);

		if (bit == 0)
		{
			// Set the found bit in the inode bitmap
			set_bitmap(inode_bit_map, i);

			// Write it back to the disk
			write_bit_map('i');

			// Return if found
			return i;
		}

	}
	// This will hit only if we did not find any free inode
	return -1;
}

/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {

	// Step 1: Read data block bitmap from disk
	
	// Step 2: Traverse data block bitmap to find an available slot

	// Step 3: Update data block bitmap and write to disk 

	for(int i = 0; i < MAX_DNUM; i++)
	{
		uint8_t bit = get_bitmap(data_bit_map, i);

		if (bit == 0)
		{
			// Set the found bit in the data bitmap
			set_bitmap(data_bit_map, i);

			// Write it back to the disk
			write_bit_map('d');

			// Return if found
			return i;
		}
	}
	// This will hit only if we did not find any free data block	
	return -1;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {

// Step 1: Get the inode's on-disk block number

// Step 2: Get offset of the inode in the inode on-disk block

// Step 3: Read the block from disk and then copy into inode structure

	
	// Compute the block number where the passed inode exists on the disk
	int block_num = ino/num_inodes_per_block;

	// Compute the actual block number 
	int block = sb.i_start_blk + block_num;

	// Compute the offset
	int offset = (ino % num_inodes_per_block);

	// Allocate block size to read the block from disk
	void *buffer = malloc(BLOCK_SIZE);

	// Do a read from disk
	bio_read(block, buffer);

	struct inode * ptr;
	ptr = buffer;

	// Move the pointer to the actual location where inode is present
	ptr = ptr + offset;

	// Copy it to the inode structure
	memcpy((void *)inode, (const void *) ptr, sizeof(struct inode));

	free(buffer);
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {

	// Step 1: Get the block number where this inode resides on disk
	
	// Step 2: Get the offset in the block where this inode resides on disk

	// Step 3: Write inode to disk
	

	// Compute the block number where the inode exists on the disk
	int block_num = ino/num_inodes_per_block;

	//Going to the particular block in inode region	
	int block = sb.i_start_blk + block_num ;

	int offset = (ino % num_inodes_per_block);

	void * buffer = malloc(BLOCK_SIZE);

	bio_read(block, buffer);

	struct inode * ptr;
	ptr = buffer;

	// Move the pointer to the actual location where inode is present
	ptr = ptr + offset;

	// Copy the inode structure to buffer
	memcpy((void *)ptr, (const void *) inode, sizeof(struct inode));

	// Write back the buffer back into the disk
	bio_write(block, buffer);

	free(buffer);
	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {

  // Step 1: Call readi() to get the inode using ino (inode number of current directory)

  // Step 2: Get data block of current directory from inode

  // Step 3: Read directory's data block and check each directory entry.
  //If the name matches, then copy directory entry to dirent structure

	
	struct inode dir_inode;
	// Read the inode structure for the corresponding inode number
	readi(ino, &dir_inode);

	// Number of data blocks
	int num_data_blocks = dir_inode.size;

	// Check for directory in each data block
	for(int i = 0; i < num_data_blocks; i++)
	{
		int data_block = dir_inode.direct_ptr[i];

		// Allocate the buffer and read the data block to the buffer
		void * buffer = malloc(BLOCK_SIZE);
		bio_read(data_block, buffer);

		struct dirent * p = buffer; 

		// Check all the directory entry present in the buffer if name matches
		for (int j = 0; j < num_dirents_per_block; j++)
		{

			//Checking the validity of directory entry
			if((p+j) -> valid == 1)
			{

				char * name = (p+j) -> name;

				//compare string to check if directory entry matches with fname
				if (strcmp(name, fname) == 0)
				{
					// file is found, copy the directory entry to passed paramter and return
					memcpy((void *) dirent, (void *)(p + j), sizeof(struct dirent));
					free(buffer);
					return 1;
				}
			}
		}
		// file not found if we reach here
		free(buffer);
	}
	return 0;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	
	// Step 2: Check if fname (directory name) is already used in other entries

	// Step 3: Add directory entry in dir_inode's data block and write to disk

	// Allocate a new data block for this directory if it does not exist

	// Update directory inode

	// Write directory entry

	// Check if file name exists using the directory inode information
	int found = name_exist(dir_inode, fname);

	// Return if found
	if(found == 0)
	{
		return -1;
	}

	int present = 0;
	int num_data_blocks = dir_inode.size;

	// Get the directory entry for each data block
	for(int i = 0; i < num_data_blocks; i++)
	{
		int dataBlock = dir_inode.direct_ptr[i];

		// Allocate the memory for buffer and read the corresponding data block from the disk
		void * buffer = malloc(BLOCK_SIZE);
		bio_read(dataBlock, buffer);

		struct dirent * p = buffer; 

		// Check if we are able to find any free directory entry to add
		for (int j = 0; j < num_dirents_per_block; j++)
		{
			if((p+j) -> valid == 0)
			{
				// We found a directory entry to add, mark the check variable!
				present = 1;

				struct dirent * addr = p+j;
				addr -> ino = f_ino;
				addr -> valid = 1;
				strcpy(addr->name, fname);
				addr -> len = name_len;

				//write this buffer back to the disk
				bio_write(dataBlock, buffer);

				// Break out of for loops using below conditions
				j = num_dirents_per_block+1;
				i = num_data_blocks + 1;
			}
		}
		free(buffer);
	}
	// This means we were not able to find an directory to add and we need a new block to add this entry
	if(present == 0)
	{
		// Get the next available data block to add this directory
		int new_block = get_avail_blkno() + sb.d_start_blk;

		// Format this block to hold directory structures
		format_block_to_dirents (new_block);

		// Allocate memory to read directoruy inode structure 
		struct inode * new_dir_inode = (struct inode *)malloc(sizeof(struct inode)); 

		// Reading the directory inode from the disk 
		readi(dir_inode.ino, new_dir_inode);
		// Update the size
		new_dir_inode -> size += 1;

		// Update the direct pointer information to hold the new block which we found
		*((new_dir_inode -> direct_ptr) + num_data_blocks) = new_block;

		// Write this updated info back to disk
		writei(dir_inode.ino, new_dir_inode);

		void * buffer = malloc(BLOCK_SIZE);

		// Read the new block info into the buffer
		bio_read(new_block, buffer);

		struct dirent * addr = buffer;
		addr -> ino = f_ino;
		addr -> valid = 1;
		strcpy(addr->name, fname);
		addr -> len = name_len;

		// Write back into the disk after updating the infomation
		bio_write(new_block, buffer);

		free(buffer);

		free(new_dir_inode);
	}
	return 0;
}

int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	// Get number of data blocks present in the directory inode
	int num_data_blocks = dir_inode.size;

	// Check for each data block to find where the file name exist
	for(int i = 0; i < num_data_blocks; i++)
	{

		int data_block = dir_inode.direct_ptr[i];
		
		// Allocae the buffer to read the block from the disk
		void * buffer = malloc(BLOCK_SIZE);
		// Read the correspondoing data block from the disk
		bio_read(data_block, buffer);

		struct dirent * p = buffer; 

		// Check in each directory entry whether we can find the file
		for (int j = 0; j < num_dirents_per_block; j++)
		{

			if((p+j) -> valid == 1)
			{
				char * name = (p+j) -> name;
				// Doing a string comparison
				if (strcmp(name, fname) == 0)
				{

					// File found, now update the block after making it invalid and return
					(p + j) -> valid = 0;
					bio_write(data_block, buffer);
					free(buffer);
					return 1;
				}
			}
		}
		// We dint find the directory we wnated to remove if we reach here
		free(buffer);
	}
	return 0;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
	
	// Step 1: Resolve the path name, walk through path, and finally, find its inode.
	// Note: You could either implement it in a iterative way or recursive way

	// Duplicating the path in path1 and path2 so as to get the basename and parent name
	char * p1 = strdup(path);
	char * p2 = strdup(path);
	
	// Get the base name and parent directory name using linux provides APIS
	char * base_name = basename(p1);
	char * dir_name = dirname(p2);
	
	// Base case in a recursive call, which is the root directory
	if(strcmp(base_name, "/")==0)
	{
		// Read the inode of the root directory
		readi(0, inode);
        return 0;
	}

	struct dirent * dir_ent = (struct dirent *)malloc(sizeof(struct dirent));

	struct inode * temp_inode = (struct inode *)malloc(sizeof(struct inode));

	// Recursive call by passing the parent directory name as the path
    int i =  get_node_by_path(dir_name, 0, temp_inode);

	if (i == -1)
	{
		// This means path is invalid
		free(dir_ent);
		free(temp_inode);
		return -1;
	}

	int j = dir_find(temp_inode -> ino, base_name, strlen(base_name), dir_ent);
    free(temp_inode);
	if (j == 0)
	{
		// File does not exit and we return -1
	    free(dir_ent);
        return -1;
    }
	// If we reached here, its a valid path and we get the inode number
	int inode_num = dir_ent->ino;
	
	// We populate the inode which is used by the parent recursive call again
    readi(inode_num, inode);

    free(dir_ent);

	return 0;
}

/* 
 * Make file system
 */
int rufs_mkfs() {

	// Call dev_init() to initialize (Create) Diskfile

	// write superblock information

	// initialize inode bitmap

	// initialize data block bitmap

	// update bitmap information for root directory

	// update inode for root directory

	dev_init(diskfile_path);

	// populating the super block information
	sb.magic_num = MAGIC_NUM;
	sb.max_inum = MAX_INUM;
	sb.max_dnum = MAX_DNUM;

	// Next we compute all the starting blocks of inode bitmaps, data bit maps, 
	// inode blocks and data blocks.
	sb.i_bitmap_blk = sizeof(struct superblock)/BLOCK_SIZE;

	if (sizeof(struct superblock)%BLOCK_SIZE != 0)
	{
		sb.i_bitmap_blk++;
	}

	// Compute number of blocks required to store inode bitmap.
	// Since 1 byte = 8 bits
	int num_inode_bit_map_blocks = ((MAX_INUM)/8)/BLOCK_SIZE;

	if ((((MAX_INUM)/8)%BLOCK_SIZE) !=0)
	{
		num_inode_bit_map_blocks++;
	}
	
	// Compute data bit map staring block number
	sb.d_bitmap_blk = sb.i_bitmap_blk + num_inode_bit_map_blocks;

	int num_data_bit_map_blocks = ((MAX_DNUM)/8)/BLOCK_SIZE;

	if ((((MAX_DNUM)/8)%BLOCK_SIZE) !=0)
	{
		num_data_bit_map_blocks++;
	}

	sb.i_start_blk = sb.d_bitmap_blk + num_data_bit_map_blocks;

	int num_inode_blocks = (sizeof(struct inode) * MAX_INUM)/BLOCK_SIZE;

	if ((sizeof(struct inode) * MAX_INUM)%BLOCK_SIZE !=0)
	{
		num_inode_blocks++;
	}
	sb.d_start_blk = sb.i_start_blk + num_inode_blocks;

	// Copy the populated super block to allocated super block memory
	memcpy((void *)sb_ptr, (const void *) &sb, sizeof(struct superblock));

	// Write the super block to disk at block 0
	bio_write(0, (const void *) sb_ptr);

	// Initialize inode bitmap
	for(int i = 0; i < (MAX_INUM/8); i ++)
	{
        inode_bit_map[i] = '\0';
	}
	write_bit_map('i');

	// Initialize data bitmap
	for(int i = 0; i < (MAX_DNUM/8); i ++)
	{
		data_bit_map[i] = '\0';
	}
	write_bit_map('d');

	// Set the inode bitmap for root
	set_bitmap(inode_bit_map, 0);
	// Write it to the disk
	write_bit_map('i');

	// Set the data bitmap for root
	set_bitmap(data_bit_map, 0);
	// Write it to the disk
	write_bit_map('d');

	// Since root contails all directories, we format the root data block
	// which is the first one, 
	format_block_to_dirents (sb.d_start_blk);

	// Populate the inode for root directory
	struct inode root_inode;

	root_inode.ino = 0;
	root_inode.valid = 1;
	root_inode.size = 1;
	root_inode.type = D;
	root_inode.link = 2;
	root_inode.direct_ptr[0] = sb.d_start_blk;
	(root_inode.vstat).st_atime = time(NULL);
	(root_inode.vstat).st_mtime = time(NULL);
	(root_inode.vstat).st_mode = S_IFDIR | 0755;

	// Allocate the buffer to write back into the disk the inode of root information
	void * buffer = malloc(BLOCK_SIZE);
	bio_read(sb.i_start_blk, buffer);
	
	memcpy(buffer, (const void *) &root_inode, sizeof(struct inode));
	bio_write(sb.i_start_blk, (const void *)buffer);
	free(buffer);

	// Add the dirent of '.' for the root
	const char * filename = ".";
	dir_add(root_inode, 0, filename, 1);

	return 0;
}


/* 
 * FUSE file operations
 */
static void *rufs_init(struct fuse_conn_info *conn) {

	// Step 1a: If disk file is not found, call mkfs

  // Step 1b: If disk file is found, just initialize in-memory data structures
  // and read superblock from disk


  	pthread_mutex_lock(&mutex);
	// Step 1a: If disk file is not found, call mkfs
	if(dev_open(diskfile_path) == -1){

		sb_ptr = malloc(BLOCK_SIZE);
		inode_bit_map = (bitmap_t) malloc((MAX_INUM)/8);
		data_bit_map = (bitmap_t) malloc((MAX_DNUM)/8);
		rufs_mkfs();
	}
	else
	{
		sb_ptr = malloc(BLOCK_SIZE);
		inode_bit_map = (bitmap_t) malloc((MAX_INUM)/8);
		data_bit_map = (bitmap_t) malloc((MAX_DNUM)/8);

		// Read the super block
		bio_read(0, sb_ptr);

		// Populate the suber block info
		struct superblock * ptr = sb_ptr;
		sb = *ptr;

		// Allocate memory to read the block from the disk
		void * buffer = malloc(BLOCK_SIZE);

		bio_read(sb.i_bitmap_blk,buffer);

		// Populating the inode bit map
		memcpy((void *)inode_bit_map, buffer, MAX_INUM/8);

		free(buffer);

		// Allocate memory to read the block from the disk	
		buffer = malloc(BLOCK_SIZE);

		// Read the data bitmap info from the disk
    	bio_read(sb.d_bitmap_blk,buffer);

    	memcpy((void *)data_bit_map, buffer, MAX_DNUM/8);

    	free(buffer);
	}
	pthread_mutex_unlock(&mutex);
	return NULL;
}

static void rufs_destroy(void *userdata) {

	// Step 1: De-allocate in-memory data structures

	// Step 2: Close diskfile
	
	pthread_mutex_lock(&mutex);

	// Deallocate memory for super block pointer
	free(sb_ptr);
	// Free the bit maps
	free(inode_bit_map);
	free(data_bit_map);

	//Close diskfile
	dev_close();

	pthread_mutex_unlock(&mutex);
}

static int rufs_getattr(const char *path, struct stat *stbuf) {

	// Step 1: call get_node_by_path() to get inode from path

	// Step 2: fill attribute of file into stbuf from inode

	pthread_mutex_lock(&mutex);
	
	// Allocating the memory for inode
	struct inode * path_inode = (struct inode *)malloc(sizeof(struct inode));

	// Getting the inode of the path
	int valid = get_node_by_path(path, 0, path_inode);

	if (valid == -1)
	{
		//Invaild path
		free(path_inode);
		pthread_mutex_unlock(&mutex);
		return -ENOENT;
	}

	// Populate the attributes
	if(path_inode -> type == F)
	{
		stbuf->st_mode   = S_IFREG | 0755;
	}
	else
	{
		stbuf->st_mode   = S_IFDIR | 0755;
	}

	stbuf->st_nlink  = 2;
	time(&stbuf->st_mtime);

	stbuf -> st_uid = getuid();
	stbuf -> st_gid = getgid();
	stbuf -> st_size = (path_inode -> size) * BLOCK_SIZE;

	free(path_inode);

	pthread_mutex_unlock(&mutex);
	return 0;
}

static int rufs_opendir(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

	pthread_mutex_lock(&mutex);

	struct inode * path_inode = (struct inode *)malloc(sizeof(struct inode));

	// Getting the inode of the path
	int valid = get_node_by_path(path, 0, path_inode);
	
	if (valid == -1)
	{
		//Invaild path
		free(path_inode);
		pthread_mutex_unlock(&mutex);
		return -1;
	}

	free(path_inode);
    pthread_mutex_unlock(&mutex);
    return 0;
}

static int rufs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: Read directory entries from its data blocks, and copy them to filler

	pthread_mutex_lock(&mutex);	

	struct inode * path_inode = (struct inode *)malloc(sizeof(struct inode));

	int check = get_node_by_path(path, 0, path_inode);
	
	if (check == -1)
	{
		// Invaild path
		free(path_inode);
		pthread_mutex_unlock(&mutex);
		return -1;
	}

	int num_data_blocks = path_inode -> size;

	// check all the data blocks
	for(int i = 0; i < num_data_blocks; i++)
	{
		int dataBlock = *((path_inode -> direct_ptr) + i);

		// Allocate the memory
		void * buff = malloc(BLOCK_SIZE);
		// Read the data block from the disk to the memory
		bio_read(dataBlock, buff);

		struct dirent * p = buff;

		// Check all the directory entries in the data block which is in buffer now
		for (int j = 0; j < num_dirents_per_block; j++)
		{
			//If valid copy them to filler
			if((p+j) -> valid == 1)
			{
				char * name = (p+j) -> name;
				// use filler to put all directory entries into the buffer
				filler(buffer, name, NULL, 0);
			}
		}
		free(buff);
	}
	free(path_inode);
	pthread_mutex_unlock(&mutex);
	return 0;
}


static int rufs_mkdir(const char *path, mode_t mode) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	// Step 5: Update inode for target directory

	// Step 6: Call writei() to write inode to disk

	pthread_mutex_lock(&mutex);

	char * p1 = strdup(path);
	char * p2 = strdup(path);
	char * dir_name = dirname(p1) ;
	char * base_name = basename(p2);
	
	// Step 2: Call get_node_by_path() to get inode of parent directory

	struct inode * parent_inode = (struct inode *)malloc(sizeof(struct inode));

	int valid = get_node_by_path(dir_name, 0, parent_inode);

	if (valid == -1)
	{
		// Invalid Path
		pthread_mutex_unlock(&mutex);
		return 0;
	}
	
	// Step 3:
	int inode_num = get_avail_ino();
	if(inode_num == -1)
	{
		// No Space for file to add
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory
	
	int present = dir_add(*parent_inode, inode_num, base_name, strlen(base_name));

	if(present == -1)
	{
		// It is already present, exit!
		unset_bitmap(inode_bit_map, inode_num);
		write_bit_map('i');
		pthread_mutex_unlock(&mutex);
		return -1;
	}

	// Step 5 and 6
	// Read the parent inode to update the parent inode information and write back it to the disk
	readi(parent_inode->ino, parent_inode);
	parent_inode -> link += 1;
	writei(parent_inode -> ino, parent_inode);

	// Create a new inode for the base directory

	struct inode * new_inode = (struct inode *)malloc(sizeof(struct inode));

	new_inode -> ino = inode_num;
	new_inode -> valid = 1;
	new_inode -> size = 0;
	new_inode -> type = D;
	new_inode -> link = 2;
	// Update the time
	time(&(new_inode -> vstat).st_mtime);
	(new_inode -> vstat).st_mode = RW;

	// Write back to the disk
	writei(inode_num, new_inode);

	// Creating 2 directory entries '.' and '..' for the base file and parent file
	dir_add(*new_inode, inode_num, (const char *) ".", 1);
	readi(new_inode->ino, new_inode);
	dir_add(*new_inode, parent_inode -> ino, (const char *)"..", 2);

	// Free the allocated inode's
	free(parent_inode);
	free(new_inode);
	pthread_mutex_unlock(&mutex);
	return 0;	
}

static int rufs_rmdir(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

	pthread_mutex_lock(&mutex);

	char * p1 = strdup(path);
	char * p2 = strdup(path);
	char * dir_name = dirname(p1);
    char * base_name = basename(p2);

	// Step 2
	struct inode * target_inode = (struct inode *)malloc(sizeof(struct inode));
	int valid = get_node_by_path((char *) path, 0, target_inode);
	if (valid == -1)
	{
		// Invalid Path
		pthread_mutex_unlock(&mutex);
		return -1;
	}

    int * ptr = target_inode -> direct_ptr;

    for(int i = 0; i < target_inode -> size; i++)
	{
        int * curr_block = ptr + i;
        int block_num = *curr_block;
		//Format this block number
		format_block_to_dirents(block_num);
        int index = block_num - sb.d_start_blk;
		// Unset the block in the data bitmap
        unset_bitmap(data_bit_map, index);
    }

    int index = target_inode -> ino;
	// Unset in the inode bitmap as well
    unset_bitmap(inode_bit_map, index);

    //write these bitmaps to disk
    write_bit_map('i');
    write_bit_map('d');

	// Step 5
	struct inode * parent_inode = (struct inode *)malloc(sizeof(struct inode));
    get_node_by_path(dir_name, 0, parent_inode);

	// Step 6
	dir_remove(*parent_inode, base_name, strlen(base_name));

	free(parent_inode);
	free(target_inode);
	pthread_mutex_unlock(&mutex);
	return 0;	
}

static int rufs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target file to parent directory

	// Step 5: Update inode for target file

	// Step 6: Call writei() to write inode to disk

	pthread_mutex_lock(&mutex);

	// Step 1
	char * p1 = strdup(path);
	char * p2 = strdup(path);
	
	char * dir_name = dirname(p1);
	char * base_name = basename(p2);

	// Step 2: Call get_node_by_path() to get inode of parent directory

	struct inode * parent_inode = (struct inode *)malloc(sizeof(struct inode));

	int valid = get_node_by_path(dir_name, 0, parent_inode);

	if (valid == -1)
	{
		// Invalid Path
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	// Step 3
	int inode_num = get_avail_ino();

	if(inode_num == -1)
	{
		// No Space for file to add
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	// Step 4

	int present = dir_add(*parent_inode, inode_num, base_name, strlen(base_name));

	if(present == -1)
	{
		// Exists already
		pthread_mutex_unlock(&mutex);
		return -1;
	}

	// Step 5: Update inode for target file
	struct inode * new_inode = (struct inode *)malloc(sizeof(struct inode));

	new_inode -> ino = inode_num;
	new_inode -> valid = 1;
	new_inode -> size = 0;
	new_inode -> type = F;
	new_inode -> link = 1;
	
	time(&(new_inode -> vstat).st_mtime);
	(new_inode -> vstat).st_mode = mode;
	
	// Write back into disk
	writei(inode_num, new_inode);

	free(parent_inode);
	free(new_inode);
	
	pthread_mutex_unlock(&mutex);
	return 0;
}

	

static int rufs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

	pthread_mutex_lock(&mutex);

	struct inode * parent_inode = (struct inode *)malloc(sizeof(struct inode));

	int valid = get_node_by_path(path, 0, parent_inode);

	if (valid == -1)
	{
		// Invalid Path
		free(parent_inode);
		pthread_mutex_unlock(&mutex);
		return -1;
	}
	free(parent_inode);
	pthread_mutex_unlock(&mutex);
	return 0;
}

static int rufs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: copy the correct amount of data from offset to buffer

	// Note: this function should return the amount of bytes you copied to buffer
	
	pthread_mutex_lock(&mutex);

	int ret_size = size;
	// Step 1
	struct inode * path_inode = (struct inode *)malloc(sizeof(struct inode));

    int check = get_node_by_path(path, 0, path_inode);

    if (check == -1)
	{
            // Invalid Path
            free(path_inode);
			pthread_mutex_unlock(&mutex);
            return 0;
    }

	int file_size = (path_inode -> size) * BLOCK_SIZE;
	
	//check
	if((offset > file_size) || (size > file_size))
	{
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	// Step 2
	int block_index = offset/BLOCK_SIZE;
	int start_block = *((path_inode -> direct_ptr) + block_index);
	int start_loc = offset % BLOCK_SIZE;

	if ((file_size - offset < size))
	{
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	// Step 3
	char * buff = (char *)buffer;
	
	// Copy while size > 0 and we do a block wise copy from the disk.
	while(size > 0)
	{

		int rem = BLOCK_SIZE - start_loc;

		void * buffT = malloc(BLOCK_SIZE);

		bio_read(start_block, buffT);

		char * buffTemp = (char *)buffT;

		char * addr = buffTemp + start_loc;
		
		if (size <= rem)
		{
			memcpy((void *)buff, (void *)addr, size);
			free(buffT);
			break;
		}

		memcpy((void *)buff, (void *)addr, rem);
		buff += rem;
		size = size - rem;
		start_loc = 0;
		block_index ++;
		start_block = *((path_inode -> direct_ptr) + block_index); 
		free(buffT);	
	}
	
	free(path_inode);
	pthread_mutex_unlock(&mutex);
	return ret_size;
}

static int rufs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: Write the correct amount of data from offset to disk

	// Step 4: Update the inode info and write it to disk

	// Note: this function should return the amount of bytes you write to disk

	pthread_mutex_lock(&mutex);

	int sizeT = size;
	struct inode * pathsInode = (struct inode *)malloc(sizeof(struct inode));

    int check = get_node_by_path(path, 0, pathsInode);

    if (check == -1)
	{
        // Invalid Path
        free(pathsInode);
		pthread_mutex_unlock(&mutex);
        return 0;
    }

	// Step 2: Based on size and offset, read its data blocks from disk
	
	int blockIndex = offset/BLOCK_SIZE;
    int locationToStart = offset % BLOCK_SIZE;

	// Extra blocks if any
	int currNumOfBlocks = pathsInode->size;

	//Total file size
	int totalFileSize = (blockIndex * BLOCK_SIZE) + locationToStart + size;

	// Total blocks needed
	int numOfBlocksTotal = totalFileSize / BLOCK_SIZE;
	

	if (totalFileSize % BLOCK_SIZE != 0)
	{
		numOfBlocksTotal ++;
	}

	int diff = numOfBlocksTotal - currNumOfBlocks;

	if (diff > 0)
	{
		//we need to add extra blocks
		for (int index = 0; index < diff; index ++)
		{
			int d = get_avail_blkno();
			if (d == -1)
			{
				break;
			}
			*((pathsInode->direct_ptr) + currNumOfBlocks + index) = d + sb.d_start_blk;
			pathsInode ->size ++;
		}
	}

	if (pathsInode->size == 0)
	{
		pthread_mutex_unlock(&mutex);
		return 0;
	}
	if ((pathsInode -> size) < blockIndex + 1)
	{
		pthread_mutex_unlock(&mutex);
		return 0 ;
	}

	// Step 3

	int blockToStart = *((pathsInode -> direct_ptr) + blockIndex);
	char * bufferT = (char *)buffer;

    while(size > 0)
	{

        int remaining = BLOCK_SIZE - locationToStart;

        void * buffT = malloc(BLOCK_SIZE);

        bio_read(blockToStart, buffT);

        char * buffTemp = (char *)buffT;

        char * location = buffTemp + locationToStart;

        if (size <= remaining)
		{
            memcpy((void *)location, (void *)bufferT, size);
			bio_write(blockToStart,buffT);
            free(buffT);
            break;
        }

        memcpy((void *)location, (void *)bufferT, remaining);
		bio_write(blockToStart, buffT);
                
		bufferT += remaining;
        size = size - remaining;
        locationToStart = 0;
        blockIndex ++;

		if((pathsInode -> size) < blockIndex + 1)
		{
			time(&(pathsInode -> vstat).st_mtime);
			writei(pathsInode -> ino, pathsInode);
			pthread_mutex_unlock(&mutex);
			return (sizeT - size);
		}

        blockToStart = *((pathsInode -> direct_ptr) + blockIndex);
        free(buffT);
    }

	// Step 4
	time(&(pathsInode -> vstat).st_mtime);

	writei(pathsInode -> ino, pathsInode);
	
	free(pathsInode);
	pthread_mutex_unlock(&mutex);
	return sizeT;
}

static int rufs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory

	pthread_mutex_lock(&mutex);

	// Step 1
	char * p1 = strdup(path);
	char * p2 = strdup(path);
	
	char * dir_name = dirname(p1);
	char * base_name = basename(p2);

	// Step 2
	struct inode * target_inode = (struct inode *)malloc(sizeof(struct inode));

    int valid = get_node_by_path(path, 0, target_inode);

    if (valid == -1)
	{
		// Invalid Path
        free(target_inode);
		pthread_mutex_unlock(&mutex);
        return 0;
     }

	// Step 3
	for(int i = 0; i < target_inode -> size; i++)
	{
		// Compute the block to be invalidated
		int block_to_be_removed = *((target_inode -> direct_ptr) + i);
		int dataBlockIndex = block_to_be_removed - sb.d_start_blk;
		// Unset the block 
		unset_bitmap(data_bit_map, dataBlockIndex);
	}

	// Step 4
	unset_bitmap(inode_bit_map, target_inode -> ino);
	// Update the bitmaps after un-setting both inode and data bitmaps
	write_bit_map('i');
	write_bit_map('d');
	// Mark it invalid
	target_inode -> valid = 0;
	writei(target_inode -> ino, target_inode);

	// Step 5
	struct inode * parent_dir_inode = (struct inode *)malloc(sizeof(struct inode));
	get_node_by_path(dir_name, 0, parent_dir_inode);
	
	// Step 6
	dir_remove(*parent_dir_inode, base_name, strlen(base_name));
	pthread_mutex_unlock(&mutex);

	return 0;
}

static int rufs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations rufs_ope = {
	.init		= rufs_init,
	.destroy	= rufs_destroy,

	.getattr	= rufs_getattr,
	.readdir	= rufs_readdir,
	.opendir	= rufs_opendir,
	.releasedir	= rufs_releasedir,
	.mkdir		= rufs_mkdir,
	.rmdir		= rufs_rmdir,

	.create		= rufs_create,
	.open		= rufs_open,
	.read 		= rufs_read,
	.write		= rufs_write,
	.unlink		= rufs_unlink,

	.truncate   = rufs_truncate,
	.flush      = rufs_flush,
	.utimens    = rufs_utimens,
	.release	= rufs_release
};


int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &rufs_ope, NULL);

	return fuse_stat;
}

