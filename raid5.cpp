#include "src.hpp"
#include <cstdio>
#include <iostream>
// #include <fstream>
#include <string>
#include <vector>
#include <memory>
#include <cassert>
#include <stdexcept>

// Helper to create/clear a drive file
void prepare_drive(const std::string& filename, int blocks, int block_size) {
    FILE* file = fopen(filename.c_str(), "wb");
    std::vector<char> zeros(block_size, '\0');
    for (int i = 0; i < blocks; i++) {
        fwrite(zeros.data(), 1, block_size, file);
    }
    fclose(file);
}

// Helper to open files before starting the controller
void open_files(const std::vector<std::unique_ptr<sjtu::fstream>>& files, const std::vector<std::string>& filenames) {
    for (int i = 0; i < filenames.size(); i++) {
        files[i]->open(filenames[i], std::ios::binary | std::ios::in | std::ios::out);
        if (!files[i]->is_open()) {
            throw std::runtime_error("Failed to open file: " + filenames[i]);
        }
    }
}

// Helper to generate predictable content for a block
std::string generate_block_content(int block_id, int block_size) {
    std::string content;
    content.reserve(block_size);

    char        base             = 'A' + (block_id % 26);
    std::string id_str           = std::to_string(block_id);
    int         base_chars_count = block_size - id_str.length();

    for (int i = 0; i < base_chars_count; i++) {
        content += base;
    }
    content += id_str;

    return content;
}

// Helper to verify block content
void verify_block(const char* actual, const std::string& expected, int block_size, int block_id) {
    bool failed = false;
    for (int i = 0; i < block_size; i++) {
        if (actual[i] != expected[i]) {
            failed = true;
            break;
        }
    }
    if (failed) {
        throw std::runtime_error("Block verification failed at block ID " + std::to_string(block_id) + ":\n"
            + "Expected: " + expected + "\n"
            + "Actual: " + std::string(actual, block_size));
    }
}

void run_test_scenario(int num_drives, int block_size, int blocks_per_drive) {
    std::cout << "\nTesting with " << num_drives << " drives, "
        << block_size << " bytes per block, "
        << blocks_per_drive << " blocks per drive" << std::endl;

    // Prepare drives
    std::vector<std::string>                   filenames;
    std::vector<std::unique_ptr<sjtu::fstream>> drives;
    for (int i = 0; i < num_drives; i++) {
        std::string filename = "drive_" + std::to_string(i) + ".txt";
        filenames.push_back(filename);
        prepare_drive(filename, blocks_per_drive, block_size);

        // Use a unique pointer to ensure the file is correctly closed even on error
        drives.push_back(std::make_unique<sjtu::fstream>(filename, std::ios::binary | std::ios::in | std::ios::out));
    }

    // Create controller
    std::vector<sjtu::fstream*> raid_drives;
    for (auto& drive : drives) {
        raid_drives.push_back(drive.get());
    }

    RAID5Controller raid(raid_drives, blocks_per_drive, block_size);
    raid.Start(EventType::NORMAL, 0);

    // Test buffer
    std::vector<char> read_buffer(block_size);

    // Write and read back a single block
    std::string content(block_size, '\0');
    raid.ReadBlock(0, read_buffer.data());
    // note: data in the disk should be all zero before writing
    verify_block(read_buffer.data(), content, block_size, 0);

    content = generate_block_content(0, block_size);
    raid.WriteBlock(0, content.c_str());
    raid.ReadBlock(0, read_buffer.data());
    verify_block(read_buffer.data(), content, block_size, 0);
    std::cout << "Single block write/read test passed" << std::endl;

    // Write and read several sequential blocks
    for (int i = 0; i < 5; i++) {
        content = generate_block_content(i, block_size);
        raid.WriteBlock(i, content.c_str());
    }
    for (int i = 0; i < 5; i++) {
        raid.ReadBlock(i, read_buffer.data());
        verify_block(read_buffer.data(), generate_block_content(i, block_size), block_size, i);
    }
    std::cout << "Sequential blocks write/read test passed" << std::endl;

    // Replace a drive
    raid.Shutdown();
    prepare_drive(filenames[1], blocks_per_drive, block_size);
    open_files(drives, filenames);
    raid.Start(EventType::REPLACED, 1);

    // Verify data after drive replacement
    for (int i = 0; i < 5; i++) {
        raid.ReadBlock(i, read_buffer.data());
        verify_block(read_buffer.data(), generate_block_content(i, block_size), block_size, i);
    }
    std::cout << "Drive replacement test passed" << std::endl;

    // Write more blocks
    for (int i = 5; i < 10; i++) {
        content = generate_block_content(i, block_size);
        raid.WriteBlock(i, content.c_str());
    }

    // Replace all drives sequentially
    for (int i = 0; i < num_drives; i++) {
        raid.Shutdown();
        prepare_drive(filenames[i], blocks_per_drive, block_size);
        open_files(drives, filenames);
        raid.Start(EventType::REPLACED, i);
    }

    // Verify data after drive replacement
    for (int i = 0; i < 10; i++) {
        raid.ReadBlock(i, read_buffer.data());
        verify_block(read_buffer.data(), generate_block_content(i, block_size), block_size, i);
    }
    std::cout << "All drives replacement test passed" << std::endl;

    // Simulate drive failure
    raid.Start(EventType::FAILED, 2);

    // Read in degraded mode
    for (int i = 0; i < 10; i++) {
        raid.ReadBlock(i, read_buffer.data());
        verify_block(read_buffer.data(), generate_block_content(i, block_size), block_size, i);
    }
    std::cout << "Degraded mode read test passed" << std::endl;

    // Write in degraded mode
    for (int i = 10; i < 15; i++) {
        content = generate_block_content(i, block_size);
        raid.WriteBlock(i, content.c_str());
        raid.ReadBlock(i, read_buffer.data());
        verify_block(read_buffer.data(), content, block_size, i);
    }
    std::cout << "Degraded mode write/read test passed" << std::endl;

    // Cleanup
    raid.Shutdown();
    for (const auto& filename : filenames) {
        std::remove(filename.c_str());
    }
}

int main() {
    try {
        // First scenario: 4 drives, 4 byte blocks, 32 blocks per drive
        run_test_scenario(4, 4, 32);

        // Second scenario: 5 drives, 4KB blocks, 256 blocks per drive
        run_test_scenario(5, 4096, 256);

        std::cout << "\nAll tests passed!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    return 0;
}
