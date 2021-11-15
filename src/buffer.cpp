/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

/**
 * Andy Lin
 * Aishvi Shah
 * James Zhang
 * 
 * Implements a buffer manager using the Clock Algorithm as replacement policy
 * This file contains one instance of the BufMgr and uses classes BufDesc
 * and BufHashTbl to manage the buffer pool
 * 
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

/**
 * @brief Advance clock to next frame in the buffer pool
 */
void BufMgr::advanceClock() {
    clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {
    // Access the frames by using buffDescTable at index clockHand.
    int count = 0;
    bool allocate = false;
    // might be numBufs * 2 since if release set refBit 1 and revisit then set refBit to 0 and traverse again.
    while (count < ((int)numBufs * 2)) {
        if (bufDescTable[clockHand].valid == false) {
            frame = clockHand;
            return;
        } else {
            if (bufDescTable[clockHand].refbit) {  // then set refBit back to 0 and move to next frame.
                bufDescTable[clockHand].refbit = false;
                count++;
                advanceClock();
            } else {                                        // means refBit is false.
                if (bufDescTable[clockHand].pinCnt != 0) {  // means frame is taken by another page.
                    count++;
                    advanceClock();

                } else {  // means frame is open since refBit is 0
                    allocate = true;
                    if (bufDescTable[clockHand].dirty) {  // Has been modified and pin is 0 so write to disk.
                        bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
                        bufStats.accesses = bufStats.accesses + 1;
                        bufStats.diskwrites = bufStats.diskwrites + 1;
                        hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                        bufDescTable[clockHand].clear();
                        frame = clockHand;
                        break;
                    } else {
                        frame = clockHand;
                        break;
                    }
                }
            }
        }
    }
    if (!allocate)
        throw BufferExceededException();  // nothing was allocated after traversing all frames.
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
    //look up in hash table
    try {
        FrameId frameNo;
        // Case 2: page in buffer pool, set refbit, increment pin count and return pointer
        // to frame
        hashTable.lookup(file, pageNo, frameNo);
        //refbit
        bufDescTable[frameNo].refbit = true;
        //pinCnt++
        bufDescTable[frameNo].pinCnt++;
        //increment access
        bufStats.accesses++;
        //return pointer to the frame
        page = &(bufPool[frameNo]);

    } catch (badgerdb::HashNotFoundException const&) {
        FrameId frameNo;
        // Case 1: page not found in buffer pool
        //call allocbuf
        allocBuf(frameNo);
        //call readpage
        Page currPage = file.readPage(pageNo);
        bufStats.diskreads++;
        bufPool[frameNo] = currPage;
        //insert into hash table
        hashTable.insert(file, pageNo, frameNo);
        //call set on frame
        bufDescTable[frameNo].Set(file, pageNo);
        page = &(bufPool[frameNo]);
    }
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
    FrameId frameNo;
    //decrement pin count
    try {
        hashTable.lookup(file, pageNo, frameNo);
        //if pin count is  0
        if (bufDescTable[frameNo].pinCnt == 0) {
            throw PageNotPinnedException(file.filename(), pageNo, frameNo);
        } else {
            bufDescTable[frameNo].pinCnt--;
        }

        if (dirty) {
            bufDescTable[frameNo].dirty = true;
        }
    } catch (badgerdb::HashNotFoundException const&) {
    }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
    //allocate an empty page
    Page pageNew = file.allocatePage();
    pageNo = pageNew.page_number();

    // retrieves correct frame.
    FrameId frameNew;
    allocBuf(frameNew);

    //insert page in hash table and bufPool.
    hashTable.insert(file, pageNo, frameNew);

    //call Set on frame
    bufDescTable[frameNew].Set(file, pageNo);
    bufPool[frameNew] = pageNew;
    bufStats.accesses++;

    // return the needed references
    page = &bufPool[frameNew];
}

void BufMgr::flushFile(File& file) {
    //write all dirty bit frames to the disk

    for (FrameId i = 0; i < numBufs; i++) {
        // Finds the file
        if (bufDescTable[i].file == file) {
            //throw error if any frame is invalid
            if (bufDescTable[i].valid == false) {
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
            //throw error if page is pinned
            if (bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, i);
            }

            // if page is dirty
            if (bufDescTable[i].dirty) {
                // write to disk
                bufDescTable[i].file.writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
                // remove page from hash table
                hashTable.remove(file, bufDescTable[i].pageNo);
                // clear the page frame
                bufDescTable[i].clear();
            }
        }
    }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
    // call remove - to remove from hash table - if not there
    // check if frame exists in hashTable first.
    try {
        FrameId frameNew;
        hashTable.lookup(file, PageNo, frameNew);
        // remove the frame.
        hashTable.remove(file, PageNo);
        // remove information about the frame from array.
        bufDescTable[frameNew].clear();
    } catch (HashNotFoundException& e) {
    }
    //delete page from file
    file.deletePage(PageNo);
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