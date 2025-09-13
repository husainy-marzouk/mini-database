// fastrepo_segments_wal_batch.cpp
// High-performance fixed-record repo with:
//  - WAL batching
//  - Segment-based append-only storage
//  - FD-based IO (pread/pwrite), optional mmap
//  - persistent index (.idx), free-list (.fre), id (.id), wal (.wal)
//  - background persister, WAL replay, compaction
// Build: g++ -std=c++20 -O3 -march=native -flto -pthread fastrepo_segments_wal_batch.cpp -o fastrepo

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <functional>
#include <stdexcept>
#include <iostream>
#include <algorithm>
#include <type_traits>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <cassert>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>

// ---------------- compile-time constraints ----------------
template<typename T>
concept FixedRecord = std::is_trivially_copyable_v<T> && std::is_standard_layout_v<T>;

// tombstone type explicit size
using tombstone_t = std::uint8_t;

// ---------------- low-level I/O helpers ----------------
static ssize_t write_all_fd(int fd, const void* buf, size_t len) {
    const char* p = reinterpret_cast<const char*>(buf);
    size_t written = 0;
    while (written < len) {
        ssize_t w = ::write(fd, p + written, len - written);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        written += (size_t)w;
    }
    return static_cast<ssize_t>(written);
}

static ssize_t pread_all(int fd, void* buf, size_t count, off_t offset) {
    char* p = reinterpret_cast<char*>(buf);
    size_t read = 0;
    while (read < count) {
        ssize_t r = ::pread(fd, p + read, count - read, offset + read);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) break;
        read += (size_t)r;
    }
    return static_cast<ssize_t>(read);
}

void ssize_t 

static ssize_t pwrite_all(int fd, const void* buf, size_t count, off_t offset) {
    const char* p = reinterpret_cast<const char*>(buf);
    size_t written = 0;
    while (written < count) {
        ssize_t w = ::pwrite(fd, p + written, count - written, offset + written);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        written += (size_t)w;
    }
    return static_cast<ssize_t>(written);
}

static uint64_t file_size_by_fd(int fd) {
    struct stat st{};
    if (fstat(fd, &st) != 0) return 0;
    return static_cast<uint64_t>(st.st_size);
}

// ---------------- WAL (batched) ----------------
// WAL record format: op uint8_t + payload
// op: 1=INSERT(T),2=UPDATE(T),3=DELETE(int32)
class WAL {
public:
    WAL(std::string path, bool fsync_on_write, size_t batchSize = 64*1024)
      : path_(std::move(path)), fsync_on_write_(fsync_on_write), BATCH_SIZE(batchSize)
    {
        // open for append; O_APPEND makes writes atomic w.r.t offset
        fd_ = ::open(path_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd_ < 0) throw std::runtime_error("WAL open failed: " + path_);
        buffer_.reserve(std::min(BATCH_SIZE, (size_t)4096));
    }

    ~WAL() {
        try { flush(); } catch(...) {}
        if (fd_ >= 0) ::close(fd_);
    }

    // note: these append to internal buffer; flush will write to disk
    bool append_bytes(const void* data, size_t len) {
        std::lock_guard<std::mutex> g(mu_);
        if (buffer_.size() + len > BATCH_SIZE) {
            if (!flush_unlocked()) return false;
        }
        const char* p = reinterpret_cast<const char*>(data);
        buffer_.insert(buffer_.end(), p, p + len);
        return true;
    }

    template<typename T>
    bool append_insert(const T& rec) {
        uint8_t op = 1;
        if (!append_bytes(&op, sizeof(op))) return false;
        return append_bytes(&rec, sizeof(T));
    }

    template<typename T>
    bool append_update(const T& rec) {
        uint8_t op = 2;
        if (!append_bytes(&op, sizeof(op))) return false;
        return append_bytes(&rec, sizeof(T));
    }

    bool append_delete(int32_t id) {
        uint8_t op = 3;
        if (!append_bytes(&op, sizeof(op))) return false;
        return append_bytes(&id, sizeof(id));
    }

    // flush buffer to disk (thread-safe)
    void flush() {
        std::lock_guard<std::mutex> g(mu_);
        flush_unlocked();
    }

    // replay wal: read sequentially and call handlers
    template<typename T>
    void replay(std::function<void(const T&)> onInsert,
                std::function<void(const T&)> onUpdate,
                std::function<void(int32_t)> onDelete)
    {
        // open read-only fd
        int rfd = ::open(path_.c_str(), O_RDONLY);
        if (rfd < 0) return; // no wal
        off_t pos = 0;
        while (true) {
            uint8_t op;
            ssize_t rr = pread_all(rfd, &op, sizeof(op), pos);
            if (rr == 0) break; // EOF
            if (rr != (ssize_t)sizeof(op)) break;
            pos += sizeof(op);
            if (op == 1) {
                T rec;
                if (pread_all(rfd, &rec, sizeof(rec), pos) != (ssize_t)sizeof(rec)) break;
                pos += sizeof(rec);
                onInsert(rec);
            } else if (op == 2) {
                T rec;
                if (pread_all(rfd, &rec, sizeof(rec), pos) != (ssize_t)sizeof(rec)) break;
                pos += sizeof(rec);
                onUpdate(rec);
            } else if (op == 3) {
                int32_t id;
                if (pread_all(rfd, &id, sizeof(id), pos) != (ssize_t)sizeof(id)) break;
                pos += sizeof(id);
                onDelete(id);
            } else {
                break;
            }
        }
        ::close(rfd);
    }

    // clear wal file (truncate) safely
    void clear() {
        std::lock_guard<std::mutex> g(mu_);
        if (fd_ >= 0) {
            // flush buffer first
            flush_unlocked();
            ::ftruncate(fd_, 0);
            // seek/share behavior with O_APPEND: we also close and reopen to ensure clean append position
            ::close(fd_);
            fd_ = ::open(path_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
        }
    }

private:
    bool flush_unlocked() {
        if (buffer_.empty()) return true;
        // write buffer using write (fd opened with O_APPEND)
        ssize_t w = write_all_fd(fd_, buffer_.data(), buffer_.size());
        if (w < 0) return false;
        if (fsync_on_write_) ::fsync(fd_);
        buffer_.clear();
        return true;
    }

    std::string path_;
    int fd_{-1};
    bool fsync_on_write_{false};
    const size_t BATCH_SIZE;
    std::vector<char> buffer_;
    std::mutex mu_;
};

// ---------------- SegmentManager ----------------
// Manage multiple segment files; append returns (segId, offset)
// Segment file naming: basePath + ".seg{N}"
class SegmentManager {
public:
    SegmentManager(std::string basePath, size_t maxSegmentSize = 64ull*1024*1024)
      : basePath_(std::move(basePath)), maxSegmentSize_(maxSegmentSize)
    {
        // start segment 1
        openNewSegment();
    }

    ~SegmentManager() {
        for (auto &p : fdCache_) if (p.second >= 0) ::close(p.second);
    }

    // append raw data to current segment; returns encoded (segId, offset)
    // Encoded as struct SegOffset returned separately for clarity
    struct SegOffset { int segId; uint64_t offset; };

    SegOffset append(const void* buf, size_t len) {
        std::lock_guard<std::mutex> g(mu_);
        if (currentSize_ + len > maxSegmentSize_) {
            openNewSegment();
        }
        off_t off = lseek(currentFd_, 0, SEEK_END);
        if (off < 0) throw std::runtime_error("segment lseek failed");
        if (pwrite_all(currentFd_, buf, len, off) < 0) throw std::runtime_error("segment write failed");
        currentSize_ += len;
        return { currentSegId_, static_cast<uint64_t>(off) };
    }

    // ensure fd for segment id is open (returns fd)
    int getFd(int segId) {
        std::lock_guard<std::mutex> g(mu_);
        auto it = fdCache_.find(segId);
        if (it != fdCache_.end()) return it->second;
        return openExistingSegment(segId);
    }

    // build path for a segment id
    std::string segPath(int segId) const {
        return basePath_ + ".seg" + std::to_string(segId);
    }

    int currentSegId() const { return currentSegId_; }

private:
    void openNewSegment() {
        // close current
        if (currentFd_ >= 0) ::close(currentFd_);
        ++currentSegId_;
        std::string path = segPath(currentSegId_);
        int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) throw std::runtime_error("open segment failed: " + path);
        currentFd_ = fd;
        fdCache_[currentSegId_] = fd;
        currentSize_ = 0;
    }

    int openExistingSegment(int segId) {
        std::string path = segPath(segId);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open existing segment failed: " + path);
        fdCache_[segId] = fd;
        return fd;
    }

    std::string basePath_;
    size_t maxSegmentSize_{64ull*1024*1024};
    int currentSegId_{0};
    int currentFd_{-1};
    uint64_t currentSize_{0};
    std::unordered_map<int,int> fdCache_;
    std::mutex mu_;
};

// ---------------- IndexEntry for persistence (stores segment id & offset) ----------------
struct PersistIndexEntry {
    int32_t id;
    int32_t segId;
    uint64_t offset;
    bool tombstone;
};
static_assert(sizeof(PersistIndexEntry) >= sizeof(int32_t) + sizeof(int32_t) + sizeof(uint64_t) + sizeof(bool));

// ---------------- FastRepo with segments & batched WAL ----------------
template <FixedRecord T, bool UseMMap = false>
class FastRepoSegments {
public:
    static constexpr size_t RECORD_SIZE = sizeof(T);

    FastRepoSegments(std::string basePath,
                     size_t segmentSize = 64ull*1024*1024,
                     double compactThreshold = 0.30,
                     bool persistIndexImmediate = false,
                     bool walFsyncOnWrite = true,
                     bool fsyncOnDataWrite = false,
                     size_t walBatchSize = 64*1024)
    : basePath_(std::move(basePath)),
      segman_(basePath_, segmentSize),
      compactThreshold_(compactThreshold),
      persistIndexImmediate_(persistIndexImmediate),
      wal_(basePath_ + ".wal", walFsyncOnWrite, walBatchSize),
      fsyncOnDataWrite_(fsyncOnDataWrite),
      runningBackground_(true)
    {
        // replay WAL first (if exists) -> safe recovery
        replayWAL();
        // load id, index, freelist
        loadId();
        if (!loadIndex()) rebuildIndexFromData();
        loadFreeList();
        if constexpr (UseMMap) mapIfPossible();
        // background persister
        bgThread_ = std::thread([this]{ this->backgroundPersister(); });
    }

    ~FastRepoSegments() {
        runningBackground_.store(false);
        cv_.notify_one();
        if (bgThread_.joinable()) bgThread_.join();
        wal_.flush();
        persistIndex();
        persistFreeList();
        persistId();
    }

    // Insert record: WAL batch append -> apply append to segment -> update index
    std::optional<int32_t> save(const T& rec_in) {
        std::lock_guard<std::mutex> g(mu_);
        T rec = rec_in;
        rec.id = nextId_();
        setDeletedFlag(rec, false);

        // WAL record (append to batch)
        if (!wal_.append_insert<T>(rec)) return std::nullopt;

        // append to segment
        auto so = segman_.append(&rec, RECORD_SIZE);
        // update index
        index_.push_back({rec.id, so.segId, so.offset, false});
        id2pos_[rec.id] = {so.segId, so.offset};

        if (persistIndexImmediate_) persistIndex();
        if (persistIndexImmediate_) persistFreeList();
        persistId();

        // clear WAL (safe: we just applied everything up to now)
        wal_.clear();
        return rec.id;
    }

    std::optional<T> findById(int32_t id) {
        std::lock_guard<std::mutex> g(mu_);
        auto it = id2pos_.find(id);
        if (it == id2pos_.end()) return std::nullopt;
        return readAtPos(it->second.segId, it->second.offset);
    }

    // find by predicate (scans live via index)
    std::vector<T> findBy(std::function<bool(const T&)> pred, size_t limit = SIZE_MAX) {
        std::vector<T> out;
        out.reserve(64);
        std::lock_guard<std::mutex> g(mu_);
        for (const auto &ie : index_) {
            if (ie.tombstone) continue;
            auto rec = readAtPos(ie.segId, ie.offset);
            if (!rec) continue;
            if (isDeletedFlag(*rec)) continue;
            if (pred(*rec)) {
                out.push_back(*rec);
                if (out.size() >= limit) break;
            }
        }
        return out;
    }

    void scan(std::function<bool(const T&)> cb, size_t limit = SIZE_MAX) {
        size_t cnt = 0;
        std::lock_guard<std::mutex> g(mu_);
        for (const auto &ie : index_) {
            if (ie.tombstone) continue;
            auto rec = readAtPos(ie.segId, ie.offset);
            if (!rec) continue;
            if (isDeletedFlag(*rec)) continue;
            if (!cb(*rec)) return;
            if (++cnt >= limit) return;
        }
    }

    bool updateById(int32_t id, const T& updated) {
        std::lock_guard<std::mutex> g(mu_);
        auto it = id2pos_.find(id);
        if (it == id2pos_.end()) return false;
        T tmp = updated;
        tmp.id = id;
        setDeletedFlag(tmp, false);

        // WAL update (batched)
        if (!wal_.append_update<T>(tmp)) return false;

        // overwrite in-segment (in-place)
        if (!writeAtPos(it->second.segId, it->second.offset, &tmp)) return false;
        if (fsyncOnDataWrite_) fsyncSegment(it->second.segId);

        wal_.clear();
        return true;
    }

    bool removeById(int32_t id) {
        std::lock_guard<std::mutex> g(mu_);
        auto it = id2pos_.find(id);
        if (it == id2pos_.end()) return false;
        int seg = it->second.segId;
        uint64_t off = it->second.offset;

        auto rec = readAtPos(seg, off);
        if (!rec) return false;
        if (isDeletedFlag(*rec)) return false;

        // WAL delete
        if (!wal_.append_delete(id)) return false;

        // mark deleted and write back
        T tmp = *rec;
        setDeletedFlag(tmp, true);
        if (!writeAtPos(seg, off, &tmp)) return false;
        if (fsyncOnDataWrite_) fsyncSegment(seg);

        // update in-memory structures
        id2pos_.erase(it);
        for (auto &e : index_) {
            if (!e.tombstone && e.id == id) { e.tombstone = true; break; }
        }
        freeList_.push_back({seg, off});

        if (persistIndexImmediate_) persistIndex();
        if (persistIndexImmediate_) persistFreeList();

        wal_.clear();

        if (wasteRatio() > compactThreshold_) compact();

        return true;
    }

    // Compact strategy: create new segments with live records (single compact implementation for simplicity)
    void compact() {
        std::lock_guard<std::mutex> g(mu_);
        // For simplicity: write live records into new single segment file {base}.seg_compact
        std::string tmpBase = basePath_ + ".compact";
        SegmentManager compactSeg(tmpBase, segmanMaxSize());

        std::vector<PersistIndexEntry> newIndex;
        newIndex.reserve(index_.size());
        std::unordered_map<int32_t, Pos> newMap;

        for (const auto &ie : index_) {
            if (ie.tombstone) continue;
            auto rec = readAtPos(ie.segId, ie.offset);
            if (!rec) continue;
            if (isDeletedFlag(*rec)) continue;
            auto so = compactSeg.append(&*rec, RECORD_SIZE);
            newIndex.push_back({rec->id, so.segId, so.offset, false});
            newMap[rec->id] = {so.segId, so.offset};
        }

        // rotate: move compact segments to replace existing segments (naive approach)
        // WARNING: This simplistic replacement assumes single-segment storage replacement; production-grade needs careful atomic swap.
        // We'll write new index, clear freeList, and update in-memory maps.
        index_.clear();
        id2pos_.clear();
        for (auto &ne : newIndex) {
            index_.push_back(ne);
            id2pos_[ne.id] = {ne.segId, ne.offset};
        }
        freeList_.clear();

        persistIndex();
        persistFreeList();
        // Note: We don't physically remove old segment files here; handling that safely requires more involved strategy.
    }

    // rebuild index scanning all segments
    void rebuildIndexFromData() {
        std::lock_guard<std::mutex> g(mu_);
        index_.clear();
        id2pos_.clear();
        freeList_.clear();

        // For simplicity: scan segments by trying segId from 1 to currentSegId()
        int maxSeg = segman_.currentSegId();
        for (int seg = 1; seg <= maxSeg; ++seg) {
            int fd = segman_.getFd(seg); // may throw if segment missing
            uint64_t segSize = file_size_by_fd(fd);
            uint64_t pos = 0;
            while (pos + RECORD_SIZE <= segSize) {
                T rec;
                if (pread_all(fd, &rec, RECORD_SIZE, static_cast<off_t>(pos)) != (ssize_t)RECORD_SIZE) break;
                if (!isDeletedFlag(rec)) {
                    index_.push_back({rec.id, seg, pos, false});
                    id2pos_[rec.id] = {seg, pos};
                    if (rec.id > lastId_) lastId_ = rec.id;
                } else {
                    freeList_.push_back({seg, pos});
                }
                pos += RECORD_SIZE;
            }
        }
        persistIndex();
        persistFreeList();
    }

    void setCompactThreshold(double v) { compactThreshold_ = v; }
    double getCompactThreshold() const { return compactThreshold_; }

private:
    struct Pos { int segId; uint64_t offset; };

    // low level read/write at segment
    std::optional<T> readAtPos(int segId, uint64_t offset) const {
        int fd = segman_.getFd(segId);
        T rec;
        if (pread_all(fd, &rec, RECORD_SIZE, static_cast<off_t>(offset)) != (ssize_t)RECORD_SIZE) return std::nullopt;
        return rec;
    }

    bool writeAtPos(int segId, uint64_t offset, const T* buf) {
        int fd = segman_.getFd(segId);
        if (pwrite_all(fd, buf, RECORD_SIZE, static_cast<off_t>(offset)) < 0) return false;
        return true;
    }

    void fsyncSegment(int segId) {
        int fd = segman_.getFd(segId);
        ::fsync(fd);
    }

    // persist index/freeList/id
    bool loadIndex() {
        std::lock_guard<std::mutex> g(mu_);
        index_.clear();
        id2pos_.clear();
        int fd = ::open((basePath_ + ".idx").c_str(), O_RDONLY);
        if (fd < 0) return false;
        off_t pos = 0;
        PersistIndexEntry e;
        while (pread_all(fd, &e, sizeof(e), pos) == (ssize_t)sizeof(e)) {
            index_.push_back(e);
            if (!e.tombstone) id2pos_[e.id] = {e.segId, e.offset};
            pos += sizeof(e);
        }
        ::close(fd);
        return true;
    }

    void persistIndex() const {
        int fd = ::open((basePath_ + ".idx").c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) return;
        if (!index_.empty()) ::pwrite(fd, index_.data(), index_.size() * sizeof(PersistIndexEntry), 0);
        ::fsync(fd);
        ::close(fd);
    }

    void loadFreeList() {
        std::lock_guard<std::mutex> g(mu_);
        freeList_.clear();
        int fd = ::open((basePath_ + ".fre").c_str(), O_RDONLY);
        if (fd < 0) return;
        off_t pos = 0;
        // each free entry: int segId + uint64_t offset
        while (true) {
            int32_t segId;
            uint64_t off;
            if (pread_all(fd, &segId, sizeof(segId), pos) != (ssize_t)sizeof(segId)) break;
            pos += sizeof(segId);
            if (pread_all(fd, &off, sizeof(off), pos) != (ssize_t)sizeof(off)) break;
            pos += sizeof(off);
            freeList_.push_back({segId, off});
        }
        ::close(fd);
    }

    void persistFreeList() const {
        int fd = ::open((basePath_ + ".fre").c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) return;
        // write pairs
        for (const auto &p : freeList_) {
            int32_t segId = p.segId;
            uint64_t off = p.offset;
            ::pwrite(fd, &segId, sizeof(segId), 0); // simple approach uses single write offset 0 then subsequent appends would overwrite; better to build buffer
        }
        // simpler correct approach: build buffer then write
        ::close(fd);

        // rewrite using buffer to avoid wrong offsets
        fd = ::open((basePath_ + ".fre").c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) return;
        std::vector<char> buf;
        buf.reserve(freeList_.size() * (sizeof(int32_t)+sizeof(uint64_t)));
        for (const auto &p : freeList_) {
            int32_t segId = p.segId;
            uint64_t off = p.offset;
            const char* a = reinterpret_cast<const char*>(&segId);
            buf.insert(buf.end(), a, a + sizeof(segId));
            const char* b = reinterpret_cast<const char*>(&off);
            buf.insert(buf.end(), b, b + sizeof(off));
        }
        if (!buf.empty()) write_all_fd(fd, buf.data(), buf.size());
        ::fsync(fd);
        ::close(fd);
    }

    void loadId() {
        std::lock_guard<std::mutex> g(mu_);
        lastId_ = 0;
        int fd = ::open((basePath_ + ".id").c_str(), O_RDONLY);
        if (fd >= 0) {
            int32_t v = 0;
            if (pread_all(fd, &v, sizeof(v), 0) == (ssize_t)sizeof(v)) lastId_ = v;
            ::close(fd);
        } else {
            // infer from segments (scan)
            rebuildIndexFromData();
        }
    }

    void persistId() const {
        int fd = ::open((basePath_ + ".id").c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) return;
        ::pwrite(fd, &lastId_, sizeof(lastId_), 0);
        ::fsync(fd);
        ::close(fd);
    }

    // WAL replay uses wal_.replay above; called in ctor before loading index
    void replayWAL() {
        // apply ops onto segments and index
        wal_.replay<T>(
            [this](const T &rec) { // insert
                std::lock_guard<std::mutex> g(mu_);
                if (id2pos_.count(rec.id)) return;
                auto so = segman_.append(&rec, RECORD_SIZE);
                index_.push_back({rec.id, so.segId, so.offset, false});
                id2pos_[rec.id] = {so.segId, so.offset};
                if (rec.id > lastId_) lastId_ = rec.id;
            },
            [this](const T &rec) { // update
                std::lock_guard<std::mutex> g(mu_);
                auto it = id2pos_.find(rec.id);
                if (it != id2pos_.end()) {
                    writeAtPos(it->second.segId, it->second.offset, &rec);
                }
            },
            [this](int32_t id) { // delete
                std::lock_guard<std::mutex> g(mu_);
                auto it = id2pos_.find(id);
                if (it != id2pos_.end()) {
                    auto rec = readAtPos(it->second.segId, it->second.offset);
                    if (rec) {
                        T tmp = *rec;
                        setDeletedFlag(tmp, true);
                        writeAtPos(it->second.segId, it->second.offset, &tmp);
                        id2pos_.erase(it);
                        for (auto &ie : index_) { if (!ie.tombstone && ie.id == id) { ie.tombstone = true; break; } }
                        freeList_.push_back({it->second.segId, it->second.offset});
                    }
                }
            }
        );
        wal_.clear();
    }

    // background persister: periodically flush WAL and persist meta if not immediate
    void backgroundPersister() {
        using namespace std::chrono_literals;
        while (runningBackground_.load()) {
            std::unique_lock<std::mutex> lk(bgMu_);
            cv_.wait_for(lk, 500ms);
            wal_.flush();
            if (!persistIndexImmediate_) {
                std::lock_guard<std::mutex> g(mu_);
                persistIndex();
                persistFreeList();
                persistId();
            }
        }
    }

    void mapIfPossible() {
        // not implemented for segment-based reading (advanced: map individual segments)
    }

    // helper: write at pos using segman getFd
    bool writeAtPos(int segId, uint64_t offset, const T* rec) {
        int fd = segman_.getFd(segId);
        if (pwrite_all(fd, rec, RECORD_SIZE, static_cast<off_t>(offset)) < 0) return false;
        return true;
    }

    // appendRecord wrapper (not used, segments append directly)
    size_t segmanMaxSize() const { return segmanMaxSize_; }

    // utilities for deleted flag
    static void setDeletedFlag(T &r, bool v) {
        if constexpr (std::is_member_object_pointer_v<decltype(&T::isDeleted)>) {
            r.isDeleted = v;
        } else {
            (void)r; (void)v;
        }
    }
    static bool isDeletedFlag(const T &r) {
        if constexpr (std::is_member_object_pointer_v<decltype(&T::isDeleted)>) {
            return r.isDeleted;
        } else return false;
    }

    double wasteRatio() const {
        double freed = static_cast<double>(freeList_.size());
        double total = static_cast<double>(index_.size()) + freed;
        if (total <= 0.0) return 0.0;
        return freed / total;
    }

    // ---------------- members ----------------
    std::string basePath_;
    SegmentManager segman_;
    size_t segmanMaxSize_{64ull*1024*1024};
    double compactThreshold_{0.30};
    bool persistIndexImmediate_{false};
    WAL wal_;
    bool fsyncOnDataWrite_{false};

    std::vector<PersistIndexEntry> index_;
    std::unordered_map<int32_t, Pos> id2pos_;
    std::vector<Pos> freeList_;
    int32_t lastId_{0};

    mutable std::mutex mu_;
    // background
    std::thread bgThread_;
    std::mutex bgMu_;
    std::condition_variable cv_;
    std::atomic<bool> runningBackground_;
};

// ---------------- Example record ----------------
struct alignas(8) UserFast {
    int32_t id;
    tombstone_t isDeleted;
    char pad[3];
    char name[64];
};
static_assert(std::is_trivially_copyable_v<UserFast> && std::is_standard_layout_v<UserFast>);
static_assert(sizeof(UserFast) == sizeof(int32_t) + sizeof(tombstone_t) + 3 + 64);

// ---------------- Demo main ----------------
int main() {
    try {
        FastRepoSegments<UserFast, false> repo("users_segments", 8*1024*1024, 0.30, false, true, false, 64*1024);

        UserFast a{}; std::strcpy(a.name, "Ali");
        UserFast b{}; std::strcpy(b.name, "Mona");
        UserFast c{}; std::strcpy(c.name, "Omar");

        auto ida = repo.save(a);
        auto idb = repo.save(b);
        auto idc = repo.save(c);

        if (idb) {
            if (auto r = repo.findById(*idb)) {
                std::cout << "Found id=" << r->id << " name=" << r->name << "\n";
            }
        }

        if (idc) {
            UserFast up = *repo.findById(*idc);
            std::strcpy(up.name, "Omar Updated");
            repo.updateById(*idc, up);
        }

        repo.scan([](auto const &u){
            std::cout << "scan: " << u.id << " - " << u.name << "\n";
            return true;
        });

        if (ida) repo.removeById(*ida);

        repo.compact();

        auto results = repo.findBy([](auto const &u){ return u.name[0] == 'O' || u.name[0] == 'M'; });
        for (auto &r : results) std::cout << "query matched: " << r.id << " - " << r.name << "\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    } catch (const std::exception &ex) {
        std::fprintf(stderr, "Fatal: %s\n", ex.what());
        return 2;
    }
    return 0;
}