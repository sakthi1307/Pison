#include "../src/RecordLoader.h"
#include "../src/BitmapIterator.h"
#include "../src/BitmapConstructor.h"
#include <chrono>
#include<vector>
#include<thread>

using namespace std::chrono;
// {$.user.id, $.retweet_count}
using namespace std;

string op = "";

string query(BitmapIterator* iter) {
    string output = "";
    if (iter->isObject()) {
        unordered_set<char*> set;
        set.insert("user");
        set.insert("retweet_count");
        char* key = NULL;
        while ((key = iter->moveToKey(set)) != NULL) {
            if (strcmp(key, "retweet_count") == 0) {
                // value of "retweet_count"
                char* value = iter->getValue();
                output.append(value).append(";");
                if (value) free(value);
            } else {
                if (iter->down() == false) continue;  /* value of "user" */
                if (iter->isObject() && iter->moveToKey("id")) {
                    // value of "id"
                    char* value = iter->getValue();
                    output.append(value).append(";");
                    if (value) free(value);
                }
                iter->up();
            }
        }
    }
    return output;
}



void solve(Record* record, int thread_num, int level_num){
    Bitmap* bm = NULL; 
    bm = BitmapConstructor::construct(record, thread_num, level_num);
    BitmapIterator* iter = BitmapConstructor::getIterator(bm);
    op.append(query(iter));
    delete iter;
    delete bm;
}




int main() {
    char* file_path = "../dataset/twitter_small_records.json";
    RecordSet* record_set = RecordLoader::loadRecords(file_path);
    if (record_set->size() == 0) {
        cout<<"record loading fails."<<endl;
        return -1;
    }
    string output = "";
    
    // fix the number of threads to 1 for small records scenario; parallel bitmap construction is TBD. 
    int thread_num = 8;  

    /* set the number of levels of bitmaps to create, either based on the
     * query or the JSON records. E.g., query $[*].user.id needs three levels
     * (level 0, 1, 2), but the record may be of more than three levels
     */
    int level_num = 2;
    auto start = high_resolution_clock::now();
    /* process the records one by one: for each one, first build bitmap, then perform 
     * the query with a bitmap iterator
     */
    int num_recs = record_set->size();
    cout<<"num of recs:"<<num_recs<<endl;
    for (int i = 0; i < num_recs;i+=thread_num) {
        vector<thread> threads((i+thread_num<num_recs)?thread_num:(num_recs-i));
        for(int j=0;j<threads.size();j++){
            threads[j] = thread(solve,(*record_set)[i+j], 1, level_num);
        }
        for(int j=0;j<threads.size();j++){
            threads[j].join();
        }
    }
    delete record_set;
    
        auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    cout<<"matches are: "<<op.size()<<endl;
  cout << "Time taken by function: "
         << duration.count() << " microseconds" << endl;
    return 0;
}
