#include "fstream.h"
#include <vector>
#include <cstring>

enum class EventType {
  NORMAL,
  FAILED,
  REPLACED
};

class RAID5Controller {
private:
  std::vector<sjtu::fstream *> drives_;
  int blocks_per_drive_;
  int block_size_;
  int num_disks_;
  int failed_disk_;  // -1 if no disk failed, otherwise the disk id that failed
  
  // Helper function to get the disk and stripe for a logical block
  void GetPhysicalLocation(int block_id, int &disk_id, int &stripe_id, int &parity_disk) {
    int group_id = block_id / (num_disks_ - 1);
    int offset_in_group = block_id % (num_disks_ - 1);
    
    stripe_id = group_id;
    parity_disk = (num_disks_ - 1 - group_id % num_disks_);
    
    // Map offset to actual disk, skipping the parity disk
    disk_id = offset_in_group;
    if (disk_id >= parity_disk) {
      disk_id++;
    }
  }
  
  // Read a physical block from disk
  void ReadPhysicalBlock(int disk_id, int stripe_id, char *buffer) {
    drives_[disk_id]->seekg(stripe_id * block_size_, std::ios::beg);
    drives_[disk_id]->read(buffer, block_size_);
  }
  
  // Write a physical block to disk
  void WritePhysicalBlock(int disk_id, int stripe_id, const char *buffer) {
    drives_[disk_id]->seekp(stripe_id * block_size_, std::ios::beg);
    drives_[disk_id]->write(buffer, block_size_);
    drives_[disk_id]->flush();
  }
  
  // XOR two blocks: result = a XOR b
  void XorBlocks(const char *a, const char *b, char *result) {
    for (int i = 0; i < block_size_; i++) {
      result[i] = a[i] ^ b[i];
    }
  }
  
  // Rebuild a disk after replacement
  void RebuildDisk(int replaced_disk) {
    std::vector<char> temp_blocks(num_disks_ * block_size_);
    
    for (int stripe = 0; stripe < blocks_per_drive_; stripe++) {
      // Read all blocks in this stripe except the replaced one
      for (int disk = 0; disk < num_disks_; disk++) {
        if (disk != replaced_disk) {
          ReadPhysicalBlock(disk, stripe, &temp_blocks[disk * block_size_]);
        }
      }
      
      // XOR all blocks to get the missing one
      std::vector<char> result(block_size_, 0);
      for (int disk = 0; disk < num_disks_; disk++) {
        if (disk != replaced_disk) {
          for (int i = 0; i < block_size_; i++) {
            result[i] ^= temp_blocks[disk * block_size_ + i];
          }
        }
      }
      
      // Write the rebuilt block
      WritePhysicalBlock(replaced_disk, stripe, result.data());
    }
  }

public:
  RAID5Controller(std::vector<sjtu::fstream *> drives, int blocks_per_drive,
                  int block_size = 4096) {
    drives_ = drives;
    blocks_per_drive_ = blocks_per_drive;
    block_size_ = block_size;
    num_disks_ = drives.size();
    failed_disk_ = -1;
  }

  void Start(EventType event_type_, int drive_id) {
    if (event_type_ == EventType::NORMAL) {
      failed_disk_ = -1;
    } else if (event_type_ == EventType::FAILED) {
      failed_disk_ = drive_id;
    } else if (event_type_ == EventType::REPLACED) {
      // Rebuild the replaced disk
      RebuildDisk(drive_id);
      failed_disk_ = -1;
    }
  }

  void Shutdown() {
    for (int i = 0; i < num_disks_; i++) {
      if (i != failed_disk_ && drives_[i]->is_open()) {
        drives_[i]->close();
      }
    }
  }

  void ReadBlock(int block_id, char *result) {
    int disk_id, stripe_id, parity_disk;
    GetPhysicalLocation(block_id, disk_id, stripe_id, parity_disk);
    
    if (failed_disk_ == -1 || failed_disk_ != disk_id) {
      // Normal read or the block is not on the failed disk
      ReadPhysicalBlock(disk_id, stripe_id, result);
    } else {
      // Need to reconstruct from parity
      std::vector<char> temp(block_size_, 0);
      std::memset(result, 0, block_size_);
      
      for (int disk = 0; disk < num_disks_; disk++) {
        if (disk != failed_disk_) {
          ReadPhysicalBlock(disk, stripe_id, temp.data());
          for (int i = 0; i < block_size_; i++) {
            result[i] ^= temp[i];
          }
        }
      }
    }
  }

  void WriteBlock(int block_id, const char *data) {
    int disk_id, stripe_id, parity_disk;
    GetPhysicalLocation(block_id, disk_id, stripe_id, parity_disk);
    
    if (failed_disk_ == -1) {
      // Normal mode: read old data and parity, update both
      std::vector<char> old_data(block_size_);
      std::vector<char> old_parity(block_size_);
      std::vector<char> new_parity(block_size_);
      
      ReadPhysicalBlock(disk_id, stripe_id, old_data.data());
      ReadPhysicalBlock(parity_disk, stripe_id, old_parity.data());
      
      // new_parity = old_parity XOR old_data XOR new_data
      for (int i = 0; i < block_size_; i++) {
        new_parity[i] = old_parity[i] ^ old_data[i] ^ data[i];
      }
      
      WritePhysicalBlock(disk_id, stripe_id, data);
      WritePhysicalBlock(parity_disk, stripe_id, new_parity.data());
    } else {
      // Degraded mode
      if (failed_disk_ == disk_id) {
        // Writing to failed disk: update parity instead
        std::vector<char> temp(block_size_, 0);
        std::vector<char> new_parity(block_size_);
        std::memcpy(new_parity.data(), data, block_size_);
        
        for (int disk = 0; disk < num_disks_; disk++) {
          if (disk != failed_disk_ && disk != parity_disk) {
            ReadPhysicalBlock(disk, stripe_id, temp.data());
            for (int i = 0; i < block_size_; i++) {
              new_parity[i] ^= temp[i];
            }
          }
        }
        
        WritePhysicalBlock(parity_disk, stripe_id, new_parity.data());
      } else if (failed_disk_ == parity_disk) {
        // Parity disk failed: just write the data
        WritePhysicalBlock(disk_id, stripe_id, data);
      } else {
        // Another disk failed: read old data, update data and reconstruct parity
        std::vector<char> old_data(block_size_);
        std::vector<char> new_parity(block_size_, 0);
        
        ReadPhysicalBlock(disk_id, stripe_id, old_data.data());
        WritePhysicalBlock(disk_id, stripe_id, data);
        
        // Reconstruct parity from all available data blocks
        for (int disk = 0; disk < num_disks_; disk++) {
          if (disk != failed_disk_ && disk != parity_disk) {
            std::vector<char> temp(block_size_);
            if (disk == disk_id) {
              std::memcpy(temp.data(), data, block_size_);
            } else {
              ReadPhysicalBlock(disk, stripe_id, temp.data());
            }
            for (int i = 0; i < block_size_; i++) {
              new_parity[i] ^= temp[i];
            }
          }
        }
        
        WritePhysicalBlock(parity_disk, stripe_id, new_parity.data());
      }
    }
  }

  int Capacity() {
    return (num_disks_ - 1) * blocks_per_drive_;
  }
};
