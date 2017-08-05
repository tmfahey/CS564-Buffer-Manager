/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"

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
  delete [] bufPool;
  delete [] bufDescTable;
  delete hashTable;
}

void BufMgr::advanceClock()
{
  clockHand=(clockHand+1)%numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
  bool allocated = false;
  uint32_t fullCount = 0;
  while(!allocated && fullCount < numBufs){
    if(bufDescTable[clockHand].pinCnt > 0){
      fullCount++;
      advanceClock();
    }else if(bufDescTable[clockHand].refbit==1){
      bufDescTable[clockHand].refbit=0;
      advanceClock();
    }else if(bufDescTable[clockHand].refbit==0 && bufDescTable[clockHand].pinCnt==0){
      frame = clockHand;
      if(bufDescTable[frame].valid){
        if(bufDescTable[frame].dirty){
          (bufDescTable[frame].file)->writePage(bufPool[frame]);
        }
        hashTable->remove(bufDescTable[frame].file, bufDescTable[frame].pageNo);
      }
      bufDescTable[frame].Clear();
      allocated = true;
    }
  }
  if(!allocated){
    throw BufferExceededException();
  } 
}
	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frameNo;
    if(!hashTable->lookup(file, pageNo, frameNo)){
      //page is not in a buffer frame yet, adding it
      //allocate space
      allocBuf(frameNo);
      //add to the buffer frame
      bufPool[frameNo] = file->readPage(pageNo);
      //add info to desctable and hashtable
      bufDescTable[frameNo].Set(file, pageNo);
      hashTable->insert(file, pageNo, frameNo);
    }else{
      //page already in buffer, inc pincnt and set refbit
      bufDescTable[frameNo].pinCnt++;
      bufDescTable[frameNo].refbit=true;
    }
    page = &bufPool[frameNo];
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId frameNo;
  if(hashTable->lookup(file, pageNo, frameNo)){
    if(bufDescTable[frameNo].pinCnt <= 0){
      throw PageNotPinnedException(file->filename(), pageNo, frameNo);
    }else{
      bufDescTable[frameNo].pinCnt--;
      if(dirty){
        bufDescTable[frameNo].dirty = true;
      }
    }
  }
  
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  // Allocate a new, empty page in the file and return the Page object.
  Page tempPage;
  tempPage = file->allocatePage();
  pageNo = tempPage.page_number();
  page = &tempPage;
  // Read the page into the buffer pool
  readPage(file, pageNo, page);
}

void BufMgr::flushFile(const File* file) 
{
  File tempFile = *file;
  // Ensure all frames assigned to file are unpinned
  for (FileIterator iter = tempFile.begin(); iter != tempFile.end(); ++iter) {
    FrameId frameNo;
    if(hashTable->lookup(file, (*iter).page_number(), frameNo)){
      if(bufDescTable[frameNo].pinCnt){
        throw PagePinnedException(tempFile.filename(), (*iter).page_number(), frameNo);
      }
      if(!bufDescTable[frameNo].valid){
        throw BadBufferException(frameNo, bufDescTable[frameNo].dirty, 
          bufDescTable[frameNo].valid, bufDescTable[frameNo].refbit);
      }
    }
  }

  // Iterate through all pages in the file.
  for (FileIterator iter = tempFile.begin(); iter != tempFile.end(); ++iter) {
    FrameId frameNo;
    if(hashTable->lookup(file, (*iter).page_number(), frameNo)){
      //if page is dirty, write it to disk
      if(bufDescTable[frameNo].dirty){
        (bufDescTable[frameNo].file)->writePage(bufPool[frameNo]);
        bufDescTable[frameNo].dirty = false;
      }
    }
  }
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameNo;
    if(hashTable->lookup(file, PageNo, frameNo)){
      //Page present in buffer, so remove it
      hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
      bufDescTable[frameNo].Clear();
    }
    //Delete page from file
    file->deletePage(PageNo);
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
