#include "../src/RecordLoader.h"
#include "../src/BitmapIterator.h"
#include "../src/BitmapConstructor.h"

// $.categoryPath[1:3].id
void query(BitmapIterator* iter, string& output, long& output_size) {
    if (iter->isObject() && iter->moveToKey("categoryPath")) {
        if (iter->down() == false) return; /* value of "categoryPath" */
        if (iter->isArray()) {
            for (int idx = 1; idx <= 2; ++idx) {
                // 2nd and 3rd elements inside "categoryPath" array
                if (iter->moveToIndex(idx)) {
                    if (iter->down() == false) continue;
                    if (iter->isObject() && iter->moveToKey("id")) {
                        // value of "id"
                        char* value = iter->getValue();
                        output.append(value);
                        output.append("|");
                        ++output_size;
                        if (value) free(value);
                    }
                    iter->up();
                }
            }
        }
        iter->up();
    }
}

int main() {
    char* file_path = "../dataset/bestbuy_sample_small_records.json";
    RecordSet* record_set = RecordLoader::loadRecords(file_path);
    if (record_set->size() == 0) {
        cout<<"record loading fails."<<endl;
        return -1;
    }
    string output;
    long output_size = 0;
    // visit each record sequentially
    int cur_idx = 0;
    int end_idx = record_set->size();
    int thread_num = 1;
    int max_level = 2;
    while (cur_idx < end_idx) {
        Bitmap* bm = BitmapConstructor::construct((*record_set)[cur_idx], thread_num, max_level);
        BitmapIterator* iter = BitmapConstructor::getIterator(bm);
        query(iter, output, output_size);
        delete iter;
        delete bm;
        ++cur_idx;
    }
    cout<<"the total number of output matches is "<<output_size<<endl;
    return 0;
}
