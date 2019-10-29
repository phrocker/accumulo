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
/**
BSD 2-Clause License

Copyright (c) 2017, moya-lang.org
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**/
#ifndef AllocatorH
#define AllocatorH

#include <memory>

namespace Moya {

template <class T, std::size_t growSize = 131072>
class MemoryPool
{
    struct Block
    {
        Block *next;
    };

    class Buffer
    {
        static const std::size_t blockSize = sizeof(T) > sizeof(Block) ? sizeof(T) : sizeof(Block);
        uint8_t data[blockSize * growSize];

        public:

            Buffer *const next;

            Buffer(Buffer *next) :
                next(next)
            {
            }

            T *getBlock(std::size_t index)
            {
                return reinterpret_cast<T *>(&data[blockSize * index]);
            }
    };

    Block *firstFreeBlock = nullptr;
    Buffer *firstBuffer = nullptr;
    std::size_t bufferedBlocks = growSize;


    public:

        MemoryPool() = default;
        MemoryPool(MemoryPool &&memoryPool) = delete;
        MemoryPool(const MemoryPool &memoryPool) = delete;
        MemoryPool operator =(MemoryPool &&memoryPool) = delete;
        MemoryPool operator =(const MemoryPool &memoryPool) = delete;

        ~MemoryPool()
        {
            while (firstBuffer) {
                Buffer *buffer = firstBuffer;
                firstBuffer = buffer->next;
                delete buffer;
            }
        }

        T *allocate()
        {
            if (firstFreeBlock) {
                Block *block = firstFreeBlock;
                firstFreeBlock = block->next;
                return reinterpret_cast<T *>(block);
            }

            if (bufferedBlocks >= growSize) {
                firstBuffer = new Buffer(firstBuffer);
                bufferedBlocks = 0;
            }

            return firstBuffer->getBlock(bufferedBlocks++);
        }

        void deallocate(T *pointer)
        {
            Block *block = reinterpret_cast<Block *>(pointer);
            block->next = firstFreeBlock;
            firstFreeBlock = block;
        }
};

template <class T, std::size_t growSize = 131072>
class Allocator : private MemoryPool<T, growSize>
{

    Allocator *copyAllocator;
    std::allocator<T> *rebindAllocator = nullptr;


    public:

        typedef std::size_t size_type;
        typedef std::ptrdiff_t difference_type;
        typedef T *pointer;
        typedef const T *const_pointer;
        typedef T &reference;
        typedef const T &const_reference;
        typedef T value_type;

        template <class U>
        struct rebind
        {
            typedef Allocator<U, growSize> other;
        };


        Allocator() = default;

        Allocator(Allocator &allocator) :
            copyAllocator(&allocator)
        {
        }

        Allocator(Allocator *allocator) :
            copyAllocator(allocator)
        {
        }

        template <class U>
        Allocator(const Allocator<U, growSize> &other)
        {
            if (!std::is_same<T, U>::value)
                rebindAllocator = new std::allocator<T>();
        }

        ~Allocator()
        {
            if (rebindAllocator)
                delete rebindAllocator;
        }


        pointer allocate(size_type n, const void *hint = 0)
        {
            if (copyAllocator)
                return copyAllocator->allocate(n, hint);

            if (rebindAllocator)
                return rebindAllocator->allocate(n, hint);

            if (n != 1 || hint)
                throw std::bad_alloc();

            return MemoryPool<T, growSize>::allocate();
        }

        void deallocate(pointer p, size_type n)
        {
            if (copyAllocator) {
                copyAllocator->deallocate(p, n);
                return;
            }

            if (rebindAllocator) {
                rebindAllocator->deallocate(p, n);
                return;
            }

            MemoryPool<T, growSize>::deallocate(p);
        }

        void construct(pointer p, const_reference val)
        {
            new (p) T(val);
        }

        void destroy(pointer p)
        {
            p->~T();
        }
};

}

#endif