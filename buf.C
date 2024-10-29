#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

// Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. 
// Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error 
//      when a dirty page was being written to disk and OK otherwise.  
// This private method will get called by the readPage() and allocPage() methods described below.
// Make sure that if the buffer frame allocated has a valid page in it, that you remove the appropriate entry from the hash table.
const Status BufMgr::allocBuf(int & frame) 
{
    int idx = 0 // to keep track of where we are in the clock
    Status resp;

    while(idx < numBufs * 2){
        // advance clock hand
        advanceClock();

        // if invalid, use immediately
        if(!bufTable[clockHand].valid) {
            frame = bufTable[clockHand].frameNo;
            return OK;
        }

        // check the ref bit if set to true set to false then increment idx and continue
        if(bufTable[clockHand].refbit) {
            bufTable[clockHand].refbit = false;
            idx++;
            continue;
        }

        // check if pinned, if so go to next frame
        if(bufTable[clockHand].pinCnt > 0) {
            idx++;
            continue;
        }

        // check dirty bit, and if set flush to disk and (pinCnt = 0, rebit = false, valid = true)
        if(bufTable[clockHand].dirty) {
            resp = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &(bufTable[clockHand]));
            if(resp != OK) {
                return UNIXERR;
            }
        }

        // otherwise use page and evict from hashtable
        // Must remove old page's mapping from hash table
        resp  = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        if(resp != OK) {
            return resp;
        }

        // get frameNo then clear the frame
        frame = bufTable[clockHand].pageNo;
        bufTable[clockHand].Clear();
        return OK;
    }
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{





}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


