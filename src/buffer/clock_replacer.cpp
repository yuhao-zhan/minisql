#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages):capacity(num_pages){}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    if ((clock_list.empty())) return false;

    auto it = clock_list.begin();
    while (true) {
        if (clock_status[*it] == 1) {
            clock_status[*it] = 0;
            it++;
            if (it == clock_list.end()) {
                it = clock_list.begin();
            }
        } else {
            *frame_id = *it;
            clock_status.erase(*it);
            clock_list.erase(it);
            return true;
        }
    }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
    auto it = find(clock_list.begin(), clock_list.end(), frame_id);
    if (it != clock_list.end()) {
        clock_list.erase(it);
        clock_status.erase(frame_id);
    }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    if (clock_status.find(frame_id) != clock_status.end()) {
        return ;
    }

    clock_list.push_back(frame_id);
    clock_status[frame_id] = 1;
}

size_t CLOCKReplacer::Size() {
    return clock_list.size();
}