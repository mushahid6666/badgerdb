/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * Mohit 9073337603 (mohit)
 * Mushahid Alam
 * Ali Hussain Hitawala
 * Purpose of the file: File to implement the methods exposed by BufMgr class
 *                      This file implements the actual buffer manager functionality.
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include <assert.h>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_table_exception.h"

namespace badgerdb {

	/**
	* This is the class constructor. Allocates an array for the buffer pool with bufs page frames an 	* d a corresponding BufDesc table. The way things are set up all frames will be in the clear sta	* te when the buffer pool is allocated. The hash table will also start out in an empty state. We 	* have provided the constructor.
	*/
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
  bufStats.clear();
}

	/**
   * Destructor of BufMgr class
   * Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table and Hashtable.
	 */	
BufMgr::~BufMgr() {
    for (FrameId i = 0; i < this->numBufs; i++) {
        if (this->bufDescTable[i].valid && this->bufDescTable[i].dirty) {
            File* file = this->bufDescTable[i].file;
            file->writePage(this->bufPool[i]);
            this->bufStats.diskwrites++;
        }
    }
    delete[] (this->bufDescTable);
    delete (this->hashTable);
    delete[] (this->bufPool);
}

	/**
   * Advance clock to next frame in the buffer pool
	 */
void BufMgr::advanceClock()
{
    this->clockHand = (this->clockHand+1)%(this->numBufs);
}

	/**
	 * Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to 	 * disk. Throws BufferExceededException if all buffer frames are pinned. This private method wil	 * l get called by the read-Page() and allocPage() methods described below. Make sure that if th	 * e buffer frame allocated has a valid page in it, you remove the appropriate entry from the ha	 * sh table. 
	 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
	 * @throws BufferExceededException If no such buffer is found which can be allocated
	 */
void BufMgr::allocBuf(FrameId& foundFrame)
{
    unsigned int totalPinnedPages = 0;
    unsigned int counter = 0;
    advanceClock();
    FrameId currClockHand = this->clockHand;
    //Clock replacement
    start_again:

    while( counter != this->numBufs) {
        if (this->bufDescTable[currClockHand].valid == true) {
            if (this->bufDescTable[currClockHand].refbit == true) {
                this->bufDescTable[currClockHand].refbit = false;
                advanceClock();
                currClockHand = this->clockHand;
                counter +=1;
                continue;
            }
            else if (this->bufDescTable[currClockHand].pinCnt != 0) {
                totalPinnedPages += 1;
                advanceClock();
                currClockHand = this->clockHand;
                counter +=1;
                continue;
            }
            else if (this->bufDescTable[currClockHand].dirty == true) {
                File *file = this->bufDescTable[currClockHand].file;
                file->writePage(this->bufPool[currClockHand]);
                this->bufStats.diskwrites++;
            }
            File *file = this->bufDescTable[currClockHand].file;
            PageId pageNo = this->bufDescTable[currClockHand].pageNo;
            this->bufDescTable[currClockHand].Clear();
            this->hashTable->remove(file, pageNo);
        }
        foundFrame = this->clockHand;
        break;
    }
    if (counter == this->numBufs) {
        if(totalPinnedPages == numBufs) {
            throw(BufferExceededException());
        }
        else{
            totalPinnedPages = 0;
            counter =0;
            goto start_again;
        }
    }

    return;
}

	/**
	 * Reads the given page from the file into a frame and returns the pointer to page.
	 * If the requested page is already present in the buffer pool pointer to that frame is returned
	 * otherwise a new frame is allocated from the buffer pool for reading the page.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number in the file to be read
	 * @param page  	Reference to page pointer. Used to fetch the Page object in which reques	 * ted page from file is read in.
	 */	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    this->bufStats.accesses++;
    FrameId frameNo;
    try {
        this->hashTable->lookup(file, pageNo, frameNo);
        this->bufDescTable[frameNo].refbit = true;
        this->bufDescTable[frameNo].pinCnt ++;
        page = bufPool+frameNo;
    }
    catch (HashNotFoundException) {
        try {
            this->allocBuf(frameNo);
            this->bufPool[frameNo] = file->readPage(pageNo);
            this->bufStats.diskreads++;
            this->hashTable->insert(file, pageNo, frameNo);
            this->bufDescTable[frameNo].Set(file, pageNo);
            page = bufPool+frameNo;
        }
        catch (BufferExceededException) {
            std::cout<<"BufferExceededException"<<std::endl;
            page=NULL;
            return;
        }
        return;
    }

}

	/**
	 * Unpin a page from memory since it is no longer required for it to remain in memory.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 * @param dirty		True if the page to be unpinned needs to be marked dirty
   * @throws  PageNotPinnedException If the page is not already pinned
	 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    this->bufStats.accesses++;
    FrameId frameNo;
    try {
        this->hashTable->lookup(file, pageNo, frameNo);
    }
    catch (HashNotFoundException){
        //Given in the spec for the assignment
        return;
    }
    if (this->bufDescTable[frameNo].pinCnt == 0) {
        throw PageNotPinnedException(file->filename(), pageNo, frameNo);
    }
    else {
        this->bufDescTable[frameNo].pinCnt --;
        if (dirty==true) {
            this->bufDescTable[frameNo].dirty = true;
        }
    }
}

	/**
	 * Writes out all dirty pages of the file to disk.
	 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
	 * Otherwise Error returned.
	 *
	 * @param file   	File object
   * @throws  PagePinnedException If any page of the file is pinned in the buffer pool
   * @throws BadBufferException If any frame allocated to the file is found to be invalid
	 */
void BufMgr::flushFile(const File* file) 
{
    this->bufStats.accesses++; //TODO: check how much to increment
    for (FrameId i = 0; i < this->numBufs; i++) {
        File* foundFile = this->bufDescTable[i].file;
        if (foundFile == file) {
            PageId pageNo = this->bufDescTable[i].pageNo;
            if (this->bufDescTable[i].valid == false) {
                throw BadBufferException(i,this->bufDescTable[i].dirty, this->bufDescTable[i].valid, this->bufDescTable[i].refbit);
            }
            if (this->bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(foundFile->filename(), pageNo, i);
            }
            else if (this->bufDescTable[i].dirty) {
                foundFile->writePage(this->bufPool[i]);
                this->bufStats.diskwrites++;
                this->bufDescTable[i].Clear();
                this->hashTable->remove(file, pageNo);
            }
        }
    }
}

	/**
	 * Allocates a new, empty page in the file and returns the Page object.
	 * The newly allocated page is also assigned a frame in the buffer pool.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
	 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
	 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    this->bufStats.accesses++;
    FrameId frameNo;
    this->allocBuf(frameNo);
    this->bufPool[frameNo] = file->allocatePage();
    pageNo = this->bufPool[frameNo].page_number();
    this->hashTable->insert(file, pageNo, frameNo);
    this->bufDescTable[frameNo].Set(file, pageNo);
    page = this->bufPool+frameNo;
}

	/**
	 * Delete page from file and also from buffer pool if present.
	 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    this->bufStats.accesses++;
    FrameId frameNo;
    try {
        this->hashTable->lookup(file, PageNo, frameNo);
    }
    catch (HashNotFoundException) {
        return;
    }

    this->hashTable->remove(file, PageNo);
    this->bufDescTable[frameNo].Clear();
    file->deletePage(PageNo);
    return;
}

	/**
   * Print member variable values.
	 */
void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}



}
