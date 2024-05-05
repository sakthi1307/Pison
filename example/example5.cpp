#include "../src/RecordLoader.h"
#include "../src/BitmapIterator.h"
#include "../src/BitmapConstructor.h"
#include <chrono>
#include <vector>
#include <thread>

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

struct arg_struct
{
    vector<Record *> records;
    int thread_num;
    int level_num;
};

void *solve(void *arguments)
{
    Bitmap *bm = NULL;
    string *op_local = new string("");
    struct arg_struct *args = (struct arg_struct *)arguments;
    for (int i = 0; i < args->records.size(); i++)
    {
        bm = BitmapConstructor::construct(args->records[i], args->thread_num, args->level_num);
        BitmapIterator *iter = BitmapConstructor::getIterator(bm);
        op_local->append(query(iter));
        delete iter;
    }
    delete bm;
    return (void*)op_local;
}

int main()
{
    char* file_path = "../dataset/twitter_small_records.json";
    RecordSet *record_set = RecordLoader::loadRecords(file_path);
    if (record_set->size() == 0)
    {
        cout << "record loading fails." << endl;
        return -1;
    }
    string output = "";

    // fix the number of threads to 1 for small records scenario; parallel bitmap construction is TBD.
    int thread_num = 4;

    /* set the number of levels of bitmaps to create, either based on the
     * query or the JSON records. E.g., query $[*].user.id needs three levels
     * (level 0, 1, 2), but the record may be of more than three levels
     */
    int level_num = 2;
    /* process the records one by one: for each one, first build bitmap, then perform
     * the query with a bitmap iterator
     */
    int num_recs = record_set->size();
    cout << "num of recs:" << num_recs << endl;
    vector<vector<Record *>> recs(thread_num);
    for (int i = 0; i < num_recs; i++)
    {
        recs[i % thread_num].push_back((*record_set)[i]);
    }
    auto start = high_resolution_clock::now();
    vector<pthread_t> threads(thread_num);
    vector<arg_struct> args(thread_num);

    for (int j = 0; j < threads.size(); j++)
    {
        args[j].level_num = level_num;
        args[j].records = recs[j];
        args[j].thread_num = 1;
        pthread_create(&threads[j], NULL, solve, (void *)&args[j]);
    }
    for (int j = 0; j < threads.size(); j++)
    {
        void* void_main_result;
        pthread_join(threads[j], &void_main_result);
        string *out = static_cast<string *>(void_main_result);
        output.append(*out);
    }

    delete record_set;
    auto stop = high_resolution_clock::now();

    auto duration = duration_cast<microseconds>(stop - start);
    cout << "matches are: " << output.size() << endl;
    cout << "Time taken by function: "
         << duration.count() << " microseconds" << endl;
    return 0;
}
