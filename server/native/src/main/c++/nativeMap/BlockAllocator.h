/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef _BLOCK_ALLOCATOR_H_
#define _BLOCK_ALLOCATOR_H_ 1

#include <iostream>
#include <iterator>
#include <string>
#include <vector>
#include <cstdlib>
#include <cstddef>
#include "Allocator.h"
#include "concurrentqueue.h"

#define PAGE_SIZE 4096L

#define MAX_SIZE 131072L

class BlockSegment {
private:
    std::vector<unsigned char> data_;
    size_t size_;
    bool returnable_;

public:
    BlockSegment(){
        size_ = 0;
        returnable_=false;
    }

    BlockSegment(const BlockSegment  &other) = delete;
    BlockSegment(BlockSegment &&other) = default;
    BlockSegment &operator =(BlockSegment &&other) = default;
    BlockSegment operator =(const BlockSegment &other) = delete;

    explicit BlockSegment(unsigned char *pos, size_t size) : data_(size), size_(size), returnable_(false){
    }

    void setReturnable(bool returnable){
        returnable_ = returnable;
    }

    bool isReturnable() const {
        // only return if we have an allocated buffer
        return returnable_ && size_ > 0;
    }

    unsigned char *get()  {
        return data_.data();
    }

    size_t size() const {
        return size_;
    }


    explicit operator uint8_t*() const {
        return static_cast<uint8_t*>(const_cast<uint8_t*>(data_.data()));
    }

    explicit operator jbyte*() const {
        return reinterpret_cast<jbyte*>(const_cast<uint8_t*>(data_.data()));
    }
};

class BlockAllocator{
public:
    static BlockAllocator *getAllocator(){
        static BlockAllocator allocator;
        return &allocator;
    }

    BlockSegment allocate(const size_t &mem){
        BlockSegment segment;
        if (mem < MAX_SIZE){
            size_t size = mem < PAGE_SIZE ? PAGE_SIZE : MAX_SIZE;
            if ((mem < PAGE_SIZE && segment_page.try_dequeue(segment)) || segments_128k.try_dequeue(segment)){
                consumed_+=size;
                return segment;
            }
            // temporary should be elided.
            segment = BlockSegment(0x00,size);
            segment.setReturnable(true);
            consumed_+=size;
            return segment;
        }
        else{
            consumed_+=mem;
            return BlockSegment(0x00,mem); // not returnable
        }

    }

    bool deallocate(BlockSegment &&mem){
        if (mem.size())
            return false;
        consumed_-=mem.size();
        if (mem.isReturnable())
            return mem.size() < PAGE_SIZE ? segment_page.try_enqueue(std::move(mem)) : segments_128k.try_enqueue(std::move(mem));
        else
            return false;
    }

    int64_t getMemoryUsed(){
        return consumed_;
    }

private:

    BlockAllocator() :consumed_(0){
    }

    std::atomic<int64_t> consumed_;

    moodycamel::ConcurrentQueue<BlockSegment> segment_page;

    moodycamel::ConcurrentQueue<BlockSegment> segments_128k;
};
#endif
