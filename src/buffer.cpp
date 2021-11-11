/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  if BufMgr.numBufs;
  clockHand += 1;
  bufs = bufs + 1;
}

void BufMgr::allocBuf(FrameId& frame) {}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {


}

void BufMgr::flushFile(File& file) {
  //write all dirty bit frames to the disk 
   //throw error if any pafe is pinned 
   
for (FrameId i = 0; i < bufs; i++) {
//chekc ig page is dirty - write to disk 
    if (bufDescTable[i].dirty == true){
      file.writePage()
      bufDescTable[i].dirty = false
    }

//remove page from hash table
    try{
      BufMgr.hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo)
    }
    catch (HashNotFoundException h){}
    }

  bufDescTable[i].Clear()

//throw error if any frame is invalid 
    if (bufDescTable[i].valid == true){
      throw BadBufferException()
    }
//throw error if page is pinned 
    if (bufDescTable[i].pin > 0) {
      throw PagePinnedException()
    }

  }



void BufMgr::disposePage(File& file, const PageId PageNo) {
//call remove - to remove from hash table - if not there 
  try{
    BufMgr.hashTable.remove(file, PageNo)
  }
  catch (HashNotFoundException h){}
//delete page from file ?
  file.deletePage(PageNo)
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
