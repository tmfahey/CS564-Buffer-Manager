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
      //page is not in a buffer frame yet, add it
      //allocate space in buffer for the page
      allocBuf(frameNo);
      Page target_page = file->readPage(pageNo);
      //add info to desctable
      bufDescTable[frameNo].Set(file, pageNo);
      hashTable->insert(file, pageNo, frameNo);
      // add page records to buffer frame
      for (PageIterator page_iter = (*page).begin();
           page_iter != (*page).end();
           ++page_iter) {
        bufPool[frameNo].insertRecord(*page_iter);
      }
    }else{
      bufDescTable[frameNo].pinCnt++;
    }
    page = &bufPool[frameNo];
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId frameNo;
  if(hashTable->lookup(file, pageNo, frameNo)){
    if(bufDescTable[frameNo].pinCnt == 0){
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
      //if page is dirty, write it to disk
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
    if(hashTable->lookup(&tempFile, (*iter).page_number(), frameNo)){
      //if page is dirty, write it to disk
      if(bufDescTable[frameNo].dirty){
        tempFile.writePage(*iter);
        bufDescTable[frameNo].dirty = false;
      }
    }
  }
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameNo;
    //Delete page from buffer pool if present
    if(hashTable->lookup(file, PageNo, frameNo)){
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
