/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
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
}


BufMgr::~BufMgr() {
    for (FrameId i = 0; i < this->numBufs; i++) {
        if (this->bufDescTable[i].valid && this->bufDescTable[i].dirty) {
            File* file = this->bufDescTable[i].file;
            PageId pageNo = this->bufDescTable[i].pageNo;
            file->writePage(this->bufPool[pageNo]);
        }
    }
    delete[](this->bufDescTable);
    delete[](this->hashTable);
    delete[](this->bufPool);
}

void BufMgr::advanceClock()
{
    this->clockHand = (this->clockHand+1)%(this->numBufs);
}

void BufMgr::runClockAlgorithm(FrameId& foundFrame) {
    FrameId origClockHand = this->clockHand;
    advanceClock();
    FrameId currClockHand = this->clockHand;
    //Clock replacement
    while(currClockHand!=origClockHand) {
        if (this->bufDescTable[currClockHand].valid == true) {
            if (this->bufDescTable[currClockHand].refbit == true) {
                this->bufDescTable[currClockHand].refbit = false;
                advanceClock();
                continue;
            }
            else if (this->bufDescTable[currClockHand].pinCnt != 0) {
                advanceClock();
                continue;
            }
            else if (this->bufDescTable[currClockHand].dirty == true) {
                File *file = this->bufDescTable[currClockHand].file;
                PageId pageNo = this->bufDescTable[currClockHand].pageNo;
                file->writePage(this->bufPool[pageNo]);
            }
        }
        foundFrame = this->clockHand;
        break;
    }
    if (currClockHand == origClockHand) {
        foundFrame = origClockHand;
    }
    return;
}

void BufMgr::allocBuf(FrameId& foundFrame)
{
    FrameId origClockHandle = this->clockHand;

    this->runClockAlgorithm(foundFrame);//updates foundFrame

    if (origClockHandle == foundFrame) {
        throw(BufferExceededException());
    }
    //runClockAlgorithm() flushes frame, if dirty
    return;
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
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
            this->hashTable->insert(file, pageNo, frameNo);
            this->bufDescTable[frameNo].Set(file, pageNo);
            page = bufPool+frameNo;
        }
        catch (BufferExceededException) {
            //TODO: What to do here?
            page=NULL;
            return;
        }
        catch (HashAlreadyPresentException) {
            //TODO: what to do here?
            page = NULL;
            return;
        }
        catch (HashTableException) {
            //TODO: what to do here?
            page = NULL;
            return;
        }
        return;
    }

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
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
        this->bufDescTable[frameNo].dirty = dirty;
    }
}

void BufMgr::flushFile(const File* file) 
{
    for (FrameId i = 0; i < this->numBufs; i++) {
        File* foundFile = this->bufDescTable[i].file;
        if (foundFile == file) { //TODO:how to compare files
            PageId pageNo = this->bufDescTable[i].pageNo;
            if (this->bufDescTable[i].valid == false) {
                throw BadBufferException(i,this->bufDescTable[i].dirty, this->bufDescTable[i].valid, this->bufDescTable[i].refbit);
            }
            if (this->bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(foundFile->filename(), pageNo, i);
            }
            else if (this->bufDescTable[i].dirty) {
                foundFile->writePage(this->bufPool[pageNo]);
                this->bufDescTable[i].Clear();
            }
        }
    }

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    FrameId frameNo;
    try {
        this->allocBuf(frameNo);
    }
    catch (BufferExceededException) {
        return;//TODO: check what to do here
    }
    this->bufPool[frameNo] = file->allocatePage();
    pageNo = this->bufPool[frameNo].page_number();
    this->hashTable->insert(file, pageNo, frameNo);
    this->bufDescTable[frameNo].Set(file, pageNo);
    page = bufPool+frameNo;
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
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
