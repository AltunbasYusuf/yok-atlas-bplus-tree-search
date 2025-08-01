#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Bizim ağacın orderı 8 olacak, hoca öyle dedi.


*/

#define ORDER 8
#define HEAP_CAPACITY 2000 //?
#define MAX_RUN 50         // bu maksimum kaç tane run dosyası çıkar belirliyor.
int splitAmount = 0;
int nodeAmount = 0;
double timeSpendforOneSearch = 0.0;

//----------------------------------------------------------------------------------------------------------------------------------------------------

typedef struct Record // bu arkadaş leaf Node olan bölüm isiminin üniversitelere göre puanlarını büyükten küçüğe tutacak bir linkedlist elemanı.
{
    char university[100];
    char department[100];
    float score;
    struct Record *next; // kendisinden daha düşük puanlı aynı bölümden bir sonraki üniversite adı ve puanı.
    int frozen;          // bu arkadaş bu recordun dondurulmuş mu belli etmek için var.
    char uuid[100];

} Record;

typedef struct Node // bu arkadaş B+ tree'nin leaf ya da iç node yapısını oluşturan struct.
{
    int isLeaf;  // bu nodeun leaf mi yoksa iç düğüm mü olduğunu tutar.
    int numKeys; // anlık olarak node'un kendi içibde kaç tane key tuttuğunun sayısını tutar.

    char *keys[ORDER - 1]; // içindeki elemanların adı. mesela: "Computer Engineering","Electrical Engineering"... her biri iç düğümse sadece string tutuyor.

    Record *records[ORDER - 1]; // eğer leaf nodesa tuttuğu bölümün puanlara göre azalan linkedlisti.

    struct Node *children[ORDER];

    struct Node *next; // leaf node ise bir sonraki bölüm adı olan leafı tutar.
} Node;

typedef struct MergeHeapEntry // bu struct merge ederken kullandığımız min heap'e eklencek yeni elemanın hangi run file'indan eklenmesini bulmak için kullandığımız bir yapıdır.
{
    Record *record;
    int fileFrom;
} MergeHeapEntry;

Node *root = NULL;

Record *heap[HEAP_CAPACITY];
MergeHeapEntry *mergeHeap[HEAP_CAPACITY];

int heapSize = 0;
int mergeHeapSize = 0;

// Sona eklenmiş \n, \r gibi karakterleri temizler
void trimNewline(char *str)
{
    str[strcspn(str, "\r\n")] = '\0';
}

// Baştaki ve sondaki boşlukları temizler
void trimSpaces(char *str)
{
    int len = strlen(str);
    while (len > 0 && str[len - 1] == ' ')
        str[--len] = '\0';
    int start = 0;
    while (str[start] == ' ')
        start++;
    if (start > 0)
        memmove(str, str + start, strlen(str + start) + 1);
}

Node *createNode(int isLeaf)
{
    Node *node = (Node *)malloc(sizeof(Node));
    nodeAmount++;
    node->isLeaf = isLeaf;
    node->numKeys = 0;
    node->next = NULL;
    for (int i = 0; i < ORDER; i++)
        node->children[i] = NULL;
    for (int i = 0; i < ORDER - 1; i++)
    {
        node->keys[i] = NULL;
        node->records[i] = NULL;
    }
    return node;
}

void insertNonFull(Node *node, char *key, Record *record);

void splitChild(Node *parent, int index)
{
    splitAmount++;
    Node *child = parent->children[index];      // önceden var olan ve bölünen node
    Node *newChild = createNode(child->isLeaf); // yeni bölünmüş ve oluşturulmuş olan node
    int mid = (ORDER - 1) / 2;

    newChild->numKeys = child->numKeys - mid; // yeni node'un bölünmeden sonra kaç tane key içerdiğinin sayısı

    if (child->isLeaf)
    {
        newChild->numKeys = child->numKeys - mid;

        // burada eski child node'unun ortadaki elemandan sonraki elemanları içlerinde tuttukları record bilgileri ile newChild'in içine atılıyor.
        for (int i = 0; i < newChild->numKeys; i++)
        {
            newChild->keys[i] = child->keys[mid + i];
            newChild->records[i] = child->records[mid + i];
        }
        newChild->next = child->next;
        child->next = newChild;
    }
    else // iç düğümse
    {
        newChild->numKeys = child->numKeys - mid - 1;
        for (int i = 0; i < newChild->numKeys; i++)
        {
            newChild->keys[i] = child->keys[mid + 1 + i];
        }
        for (int i = 0; i <= newChild->numKeys; i++)
        {
            newChild->children[i] = child->children[mid + 1 + i];
        }
    }

    child->numKeys = mid; // artık child nodeunun eskiden olduğunun yarısı kadar elemanı var.

    for (int i = parent->numKeys; i > index; i--) // burası da yukarı çıkan mid elemanının parent node'unun uygun yerine yerleştirilmesinin yapıldığı yer.
    {
        parent->children[i + 1] = parent->children[i];
        parent->keys[i] = parent->keys[i - 1];
    }
    parent->children[index + 1] = newChild;
    parent->keys[index] = strdup(newChild->keys[0]);
    parent->numKeys++;
}

// Bu fonksiyon, ağacın yaprak seviyelerinde verilen 'key' (bölüm adı) mevcut mu diye kontrol eder.
// Eğer bulursa o node'un adresini (outNode) ve key'in index'ini (outIndex) döner.
// Eğer bulamazsa 0 döner.
// Burada iki yıldızlı (Node**) parametre kullanılmasının sebebi, fonksiyonun dışında da değeri değiştirebilmemizdir.
// Yani dışarıya "bulduğum node burada" demek istiyoruz, bu yüzden pointer'ın kendisini değiştiriyoruz.
int findLeafAndIndex(char *key, Node **outNode, int *outIndex)
{
    Node *current = root;
    while (!current->isLeaf)
    {
        int i = 0;
        while (i < current->numKeys && strcmp(key, current->keys[i]) > 0)
            i++;
        if (current->children[i] == NULL)
        {
            fprintf(stderr, " HATA: child NULL (key: %s)", key);
            exit(EXIT_FAILURE);
        }
        current = current->children[i];
    }

    // 🔁 Şimdi leaf'leri gezerek arayalım
    while (current != NULL)
    {
        for (int i = 0; i < current->numKeys; i++)
        {
            if (strcmp(current->keys[i], key) == 0)
            {
                *outNode = current;
                *outIndex = i;
                return 1;
            }
        }
        current = current->next; // sonraki leaf'e geç
    }

    return 0;
}

// Bu fonksiyon, ağaç boşsa root oluşturur, key varsa kayıt listesine ekler, yoksa yeni kayıt eklemeyi başlatır.
// Eklenen kayıtlar, aynı key'e sahipse skor sırasına göre bağlı listeye yerleştirilir.
// Eğer root doluysa split edilir ve yeni root oluşturulur.
void insert(char *key, Record *record)
{
    trimSpaces(key);
    trimNewline(key);
    trimSpaces(record->department);
    trimNewline(record->department);

    // printf("[DEBUG] insert edilen: '%s' (%s) - %.2f\n", key, record->university, record->score); TEST İÇİN

    // root NULL'sa önce kontrol
    if (root == NULL)
    {
        root = createNode(1);
        root->keys[0] = strdup(key);
        root->records[0] = record;
        root->numKeys = 1;
        return;
    }

    // 🔍 Ancak root varsa findLeafAndIndex çağrılabilir
    Node *existingNode;
    int existingIndex;

    if (findLeafAndIndex(key, &existingNode, &existingIndex))
    {
        Record *head = existingNode->records[existingIndex];
        if (head == NULL)
        {
            existingNode->records[existingIndex] = record;
            return;
        }

        if (record->score > head->score ||
            (record->score == head->score && strcmp(record->university, head->university) < 0))
        {
            record->next = head;
            existingNode->records[existingIndex] = record;
        }
        else
        {
            Record *temp = head;
            while (temp->next &&
                   (temp->next->score > record->score ||
                    (temp->next->score == record->score &&
                     strcmp(temp->next->university, record->university) < 0)))
            {
                temp = temp->next;
            }
            record->next = temp->next;
            temp->next = record;
        }
        return;
    }

    if (root->numKeys == ORDER - 1)
    {
        Node *newRoot = createNode(0);
        newRoot->children[0] = root;
        splitChild(newRoot, 0);
        root = newRoot;
    }

    insertNonFull(root, key, record);
}

// bu fonksiyon eğer node içi dolu değilse o node'a bir şey eklemek için var.
void insertNonFull(Node *node, char *key, Record *record)
{
    trimSpaces(key);
    trimNewline(key);
    if (node->isLeaf) // eğer leaf node ise
    {
        for (int j = 0; j < node->numKeys; j++)
        {
            if (strcmp(key, node->keys[j]) == 0)
            {
                Record *head = node->records[j];
                if (record->score > head->score ||
                    (record->score == head->score && strcmp(record->university, head->university) < 0))
                {
                    record->next = head;
                    node->records[j] = record;
                }
                else
                {
                    Record *temp = head;
                    while (temp->next &&
                           (temp->next->score > record->score ||
                            (temp->next->score == record->score &&
                             strcmp(temp->next->university, record->university) < 0)))
                    {
                        temp = temp->next;
                    }
                    record->next = temp->next;
                    temp->next = record;
                }
                return;
            }
        }
        int i = node->numKeys - 1;
        while (i >= 0 && strcmp(key, node->keys[i]) < 0)
        {
            node->keys[i + 1] = node->keys[i];
            node->records[i + 1] = node->records[i];
            i--;
        }
        node->keys[i + 1] = strdup(key);
        node->records[i + 1] = record;
        node->numKeys++;
    }
    else // eğer iç düğümlerden biriyse
    {
        int i = node->numKeys - 1;
        while (i >= 0 && strcmp(key, node->keys[i]) < 0)
            i--;
        i++;
        if (node->children[i]->numKeys == ORDER - 1)
        {
            splitChild(node, i);
            if (strcmp(key, node->keys[i]) > 0)
                i++;
        }
        insertNonFull(node->children[i], key, record);
    }
}

// Belirli bir bölüm adı ve üniversite sırasına göre arama yapar
double search(char *departmentName, int rank)
{
    if (rank < 1)
    {
        printf("Invalid rank number.\n");
        return -1.0;
    }

    // temizlik için
    trimSpaces(departmentName);
    trimNewline(departmentName);

    Node *current = root;
    // bulma süresi başlangıcı
    clock_t start = clock();

    while (!current->isLeaf) // leaf değilse leafe kadar in
    {
        int i = 0;
        while (i < current->numKeys && strcmp(departmentName, current->keys[i]) > 0)
            i++;
        current = current->children[i];
    }

    while (current != NULL) // leaf nodedan ötesine geçemesin diye
    {
        for (int i = 0; i < current->numKeys; i++)
        {
            if (strcmp(current->keys[i], departmentName) == 0) // eğer istenilen bölüm adı bulunduysa
            {
                Record *record = current->records[i];
                int pos = 1;
                while (record && pos < rank)
                {
                    record = record->next;
                    pos++;
                }

                // Bulunanı ekrana yazdırmak
                if (record)
                {
                    printf("%s ranked %d in %s with score %.2f\n",
                           record->university, rank, departmentName, record->score);
                }
                else
                {
                    printf("No university found at rank %d for department %s\n", rank, departmentName);
                }

                clock_t end = clock();
                timeSpendforOneSearch = (double)(end - start) * 1000000 / CLOCKS_PER_SEC;
                return timeSpendforOneSearch;
            }
        }
        current = current->next; // bulunmadıysa leaf node elemanları içinden bir sonrakine geç
    }

    printf("Department %s not found in the tree.\n", departmentName);
    clock_t end = clock();

    timeSpendforOneSearch = (double)(end - start) * 1000000 / CLOCKS_PER_SEC;
    return timeSpendforOneSearch;
}

// Ağacı yazdırma
void printTree(Node *node, int level) // test için yazdım
{
    if (!node)
        return;

    for (int i = 0; i < level; i++)
        printf("    "); // girinti
    printf("[Level %d] ", level);

    for (int i = 0; i < node->numKeys; i++)
    {
        printf("%s", node->keys[i]);
        if (i != node->numKeys - 1)
            printf(" | ");
    }
    printf("\n");

    if (!node->isLeaf)
    {
        for (int i = 0; i <= node->numKeys; i++)
        {
            printTree(node->children[i], level + 1);
        }
    }
}

int getTreeHeight(Node *node)
{ // bu ağacın uzunluğunu hesaplıyor.
    int height = 0;
    while (node && !node->isLeaf) // leaf node ile karşılaşana kadar ilk çocuğuna atlıyor.
    {
        node = node->children[0];
        height++;
    }
    return height + 1; // leaf seviyesini de dahil et
}

//-------------------------------------------------------------------------------------------------------------------------------------------------
// bir csv dosyasından okuma yapmak-----------------------------------------------------------------------------------------------------------------

Record *readOneRecordCsvFile(FILE *f) // bu fonksiyon o csv file'ın her bir satırını okur ve record'a atar.
{
    char line[512];
    if (!f)
    {
        printf("The Csv file does not open\n");
        exit(1);
    }

    if (fgets(line, sizeof(line), f) == NULL)
        return NULL;

    Record *rec = malloc(sizeof(Record));
    rec->next = NULL;
    rec->frozen = 0;

    char *fields[4];
    int inQuotes = 0, fieldIndex = 0;
    char *ptr = line, *start = line;

    while (*ptr && fieldIndex < 4)
    {
        if (*ptr == '"')
        {
            inQuotes = !inQuotes;
        }
        else if (*ptr == ',' && !inQuotes)
        {
            *ptr = '\0';
            fields[fieldIndex++] = start;
            start = ptr + 1;
        }
        ptr++;
    }
    fields[fieldIndex++] = start;

    // Temizlik ve atama
    for (int i = 0; i < 4; i++)
    {
        trimNewline(fields[i]);
        trimSpaces(fields[i]);

        // Eğer tırnaklı ise baştaki ve sondaki tırnağı sil BU BAYA ÖNEMLİ
        if (fields[i][0] == '"')
        {
            memmove(fields[i], fields[i] + 1, strlen(fields[i]));
        }
        if (fields[i][strlen(fields[i]) - 1] == '"')
        {
            fields[i][strlen(fields[i]) - 1] = '\0';
        }
    }

    // bulunan şeyleri gerekli yerlere atamak
    strcpy(rec->uuid, fields[0]);
    strcpy(rec->university, fields[1]);
    strcpy(rec->department, fields[2]);
    rec->score = atof(fields[3]);

    printf("Reading: Department: %s | University: %s | Score: %.2f\n", rec->department, rec->university, rec->score); // burası test için

    return rec;
}

//-------------------------------------------------------------------------------------------------------------------
// Min Heap

int compareRecords(Record *a, Record *b) // iki tane recordu önce departman isimlerine göre karşılaştırır, eğer 1. ismi 2.ye göre önde geliyorsa -1 döndürür aksinde 1 döndürür.
{                                        // eğer departman adları aynıysa bu sefer de öne geçmesi gerekn score'u büyük olandır.
    int deptCmp = strcmp(a->department, b->department);

    if (deptCmp < 0)
        return -1; // a.department < b.department
    else if (deptCmp > 0)
        return 1; // a.department > b.department
    else
    {
        // Departman isimleri eşit → Score büyük olan önce gelmeli
        if (a->score > b->score)
            return -1; // a.score > b.score → a önce gelir
        else if (a->score < b->score)
            return 1;
        else
            return 0; // ikisi de aynı
    }
}

void heapifyUp(int index) // heape yeni eleman eklendiğinde parantleri ile kendsinin değerini karşılaştırarak heap'i düzgün sırasına koymaya yarar.
{
    if (index == 0)
        return;
    int parent = (index - 1) / 2; // bir node'un parent'ı onun bulunduğu indexin yarsının indexinde oluyor.
    if (compareRecords(heap[index], heap[parent]) < 0)
    {
        Record *temp = heap[index];
        heap[index] = heap[parent];
        heap[parent] = temp;
        heapifyUp(parent); // bu recursive çağrı sonradan eklenen elemanın bir yer değiştirme sonrası hala yer değiştirmeye ihtiyacı varsa diye var.
    }
}

void heapifyDown(int index) /*heapten en küçük eleman çıkarıldıktan sonra heapin rootuna en son eleman getirilir.
                            bu heapin yapısını bozar. Bu metod da bu durumu düzeltir, en yukardan itibaren nodelar yerinin bulana kadar bu işlemler tekrarlanır.*/
{
    int smallest = index;
    int left = 2 * index + 1;
    int right = 2 * index + 2;

    if (left < heapSize && compareRecords(heap[left], heap[smallest]) < 0)
        smallest = left;
    if (right < heapSize && compareRecords(heap[right], heap[smallest]) < 0)
        smallest = right;

    if (smallest != index)
    {
        Record *temp = heap[index];
        heap[index] = heap[smallest];
        heap[smallest] = temp;
        heapifyDown(smallest);
    }
}

void insertHeap(Record *rec) // heap'e bir eleman eklemek.
{
    if (heapSize == HEAP_CAPACITY)
    {
        printf("Heap dolu!\n");
        return;
    }
    heap[heapSize] = rec;
    heapifyUp(heapSize); // eleman ekledikten sonra heap'i tekrar düzenlemek.
    heapSize++;
}

Record *extractMin() // hen küçük elemanı heapten çıkarmak.
{
    if (heapSize == 0)
        return NULL;

    // En küçük frozen olmayanı bul
    int minIndex = -1;
    for (int i = 0; i < heapSize; i++)
    {
        if (!heap[i]->frozen)
        {
            if (minIndex == -1 || compareRecords(heap[i], heap[minIndex]) < 0)
                minIndex = i;
        }
    }

    if (minIndex == -1)
        return NULL; // hepsi frozen

    Record *min = heap[minIndex];

    // Heap'ten bu elemanı çıkar
    heap[minIndex] = heap[heapSize - 1];
    heapSize--;

    heapifyDown(minIndex); // yeni elemana göre düzenle

    return min;
}

//-------------------------------------------------------------------------------------------------------------------------------------------------------------------
// Min Heap (MergeHeapEntry)-----------------------------------------------------------------------------------------------------------------------------------------
// bu min heap bulk loadda run filelarını birleştrimede raharlık olsun diye adapte edilmiş versiyonu
void heapifyUpMerge(int index)
{
    if (index == 0)
        return;
    int parent = (index - 1) / 2;
    if (compareRecords(mergeHeap[index]->record, mergeHeap[parent]->record) < 0)
    {
        MergeHeapEntry *temp = mergeHeap[index];
        mergeHeap[index] = mergeHeap[parent];
        mergeHeap[parent] = temp;
        heapifyUpMerge(parent);
    }
}

void heapifyDownMerge(int index)
{
    int smallest = index;
    int left = 2 * index + 1;
    int right = 2 * index + 2;

    if (left < mergeHeapSize && compareRecords(mergeHeap[left]->record, mergeHeap[smallest]->record) < 0)
        smallest = left;
    if (right < mergeHeapSize && compareRecords(mergeHeap[right]->record, mergeHeap[smallest]->record) < 0)
        smallest = right;

    if (smallest != index)
    {
        MergeHeapEntry *temp = mergeHeap[index];
        mergeHeap[index] = mergeHeap[smallest];
        mergeHeap[smallest] = temp;
        heapifyDownMerge(smallest);
    }
}

void insertMergeHeap(MergeHeapEntry *entry)
{
    if (mergeHeapSize == HEAP_CAPACITY)
    {
        printf("MergeHeap dolu!\n");
        return;
    }
    mergeHeap[mergeHeapSize] = entry;
    heapifyUpMerge(mergeHeapSize);
    mergeHeapSize++;
}

MergeHeapEntry *extractMinMergeHeap()
{
    if (mergeHeapSize == 0)
        return NULL;
    MergeHeapEntry *min = mergeHeap[0];
    mergeHeap[0] = mergeHeap[mergeHeapSize - 1];
    mergeHeapSize--;
    heapifyDownMerge(0);
    return min;
}

//------------------------------------------------------------------------------------------------------------------------------------------------------------

// replacement sort
void replacementSort(char fileName[]) // replacement sort ile run dosyalarını oluşturuyor.
{

    int runNo = 0;
    char runName[50];

    FILE *csv = fopen(fileName, "r");

    for (int i = 0; i < HEAP_CAPACITY; i++) // en başta heap dolana kadar heap içine record ekliyoruz.
    {
        Record *newRecord = readOneRecordCsvFile(csv);
        if (!newRecord)
            break;
        newRecord->frozen = 0;
        insertHeap(newRecord);
    }

    sprintf(runName, "run_%d.csv", runNo); // bu fonksiyon strcpy gibi çalışıyor ama runNo'yu da formatlı şekilde kopyalamamıza izin veriyor.
    FILE *runFile = fopen(runName, "w");   // bu file bizim heap içinde diğer heapin bütün elemanları frozen olana kadar sıralma yaptırdıklarımızı yazdırdığımız file.

    float lastOutputScore = -1.0;
    char lastOutputDept[100] = "";

    while (heapSize > 0)
    {

        Record *currentMin = extractMin();

        Record *nextRecord = readOneRecordCsvFile(csv);

        fprintf(runFile, "%s,%s,%s,%.2f\n", currentMin->uuid, currentMin->university, currentMin->department, currentMin->score); // bu current run file'ına heapten çıkan en küçüğü yazıyor.

        if (nextRecord != NULL)
        {

            if (compareRecords(nextRecord, currentMin) < 0) // eğer heape eklenecek bir sonraki record bizim şimdiki en küçüğümüzden küçükse diğer heap'e atılmak amacıyla frozen hali açılır.
            {
                nextRecord->frozen = 1;
            }
            insertHeap(nextRecord);
        }

        // burası heap içinde her eklemeden sonra elemanların hepsninin frozen olup olmadığını kontrol ediyor.Eğer hepsi frozensa şu anki run bitmeli ve yeni run başlamalı.
        int allFrozen = 1;
        for (int i = 0; i < heapSize; i++)
        {
            if (heap[i]->frozen == 0)
            {
                allFrozen = 0;
                break;
            }
        }

        if (allFrozen == 1 && heapSize > 0) // eğer heap içindeki bütün elemanlar frozen ise şimdiki run bitmeli ve yeni run başlamalı
        {
            fclose(runFile);
            runNo++;

            sprintf(runName, "run_%d.csv", runNo); // bu fonksiyon strcpy gibi çalışıyor ama runNo'yu da formatlı şekilde kopyalamamıza izin veriyor.
            runFile = fopen(runName, "w");         // bu file bizim heap içinde diğer heapin bütün elemanları frozen olana kadar sıralma yaptırdıklarımızı yazdırdığımız file.

            for (int i = 0; i < heapSize; i++)
            {
                heap[i]->frozen = 0;
            }
        }
    }

    fclose(csv);

    // son dosya boşsa sil, bu arkadaş en sonda ekstra oluşan run dosyasını silmek için var.
    fseek(runFile, 0, SEEK_END);
    if (ftell(runFile) == 0) // dosya boyutu 0'sa boş demektir
    {
        fclose(runFile);
        remove(runName);
    }
    else
    {
        fclose(runFile);
    }
}

//------------------------------------------------------------------------------------------------------------------------------------------------

// run merge -------------------------------------------------------------------------------------------------------------------------------------

void mergeAllRunFiles()
{
    // bütün run fileları açarız, anlık olarak en üstteki elemanlarını min heap'e atarız
    /*run filelarının en küçük elemanlarını if blokları ile de karşılaştırabilirz ama eğer run file sayısı çok büyük olursa(mesela 50) 50 tane karşılaştırma yapmak çok zaman kaybı demek olur
    bundan dolayı yukarıda implemente ettiğim min heap'i yine burada kullanıyoruz, yardımcı olması için. */

    FILE *runFiles[MAX_RUN]; // en fazla 50 tane run file olsun diyelim !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    int runCount = 0;

    // bu kod bloğu şu anda kaç tane aktif run file'ı var onu buluyor. VE dosyaları açıyor.
    for (int i = 0; i < MAX_RUN; i++)
    {
        char filename[32];
        sprintf(filename, "run_%d.csv", i);

        FILE *f = fopen(filename, "r");
        if (f == NULL)
        {
            break; // Daha fazla dosya yok
        }

        runFiles[runCount++] = f; // Açılan dosyayı listeye ekle
    }

    for (int i = 0; i < runCount; i++)
    {
        Record *rec = readOneRecordCsvFile(runFiles[i]);
        if (rec == NULL)
        {
            printf("readOneRecordCsvFile dosya %d için NULL döndü (başlangıçta)\n", i);
        }
        if (rec != NULL)
        {
            MergeHeapEntry *entry = malloc(sizeof(MergeHeapEntry));
            entry->record = rec;
            entry->fileFrom = i;
            insertMergeHeap(entry);
        }
    }

    FILE *output = fopen("final_sorted.csv", "w");
    if (!output)
    {
        perror("final_sorted.csv could have not opened");
        exit(1);
    }

    while (mergeHeapSize > 0) // merge heapi doluyken final output file'ina sıralı şekilde yazmaya devam etsin
    {

        MergeHeapEntry *currentMin = extractMinMergeHeap();
        if (currentMin == NULL)
        {
            break;
            printf("hata"); // burda sıkıntı yok,Test için
        }
        Record *currentMinRec = currentMin->record;
        int fileFrom = currentMin->fileFrom; // bu arkadaş bu heape sonradan eklencek bir sonraki elemanın hangi run dosyasından geleceğini göstermesi açısından çok önemli

        if (currentMinRec && currentMinRec->university && currentMinRec->department)
        {
            fprintf(output, "%s,%s,%s,%.2f\n", currentMinRec->uuid, currentMinRec->university, currentMinRec->department, currentMinRec->score); // final dosyasına yazma
            free(currentMinRec);
        }
        else
        {
            printf("Invalid record\n");
        }

        Record *nextRec = readOneRecordCsvFile(runFiles[fileFrom]);
        if (nextRec == NULL)
        {
            printf("Run file %d has ended, no more record from the run file.\n", fileFrom);
        }

        if (nextRec != NULL)
        {
            MergeHeapEntry *newEntry = malloc(sizeof(MergeHeapEntry));
            newEntry->record = nextRec;
            newEntry->fileFrom = currentMin->fileFrom;
            insertMergeHeap(newEntry);
        }

        free(currentMin); // entry yapısını da serbest bırak
    }
    fclose(output);

    // Açık dosyaları kapat
    for (int i = 0; i < runCount; i++)
    {
        fclose(runFiles[i]);
    }
}

//------------------------------------------------------------------------------------------------------------------------------------------------

// bulk loading kısmı-----------------------------------------------------------------------------------------------------------------------------

void bulkLoadFromFinalSorted()
{
    FILE *fp = fopen("final_sorted.csv", "r");
    if (!fp)
    {
        perror("final_sorted.csv couldn't be opened");
        return;
    }

    Node *currentLeaf = createNode(1); // leaf node oluşturuyoruz.
    root = currentLeaf;                // geçici olarak root'u ilk leaf'e eşitle

    char line[512];
    int count = 0;

    while (fgets(line, sizeof(line), fp))
    {
        Record *rec = (Record *)malloc(sizeof(Record));
        rec->next = NULL;
        rec->frozen = 0;

        // Parse line
        char *part = strtok(line, ",");
        if (part)
            strcpy(rec->uuid, part);

        part = strtok(NULL, ",");
        if (part)
            strcpy(rec->university, part);

        part = strtok(NULL, ",");
        if (part)
            strcpy(rec->department, part);

        part = strtok(NULL, ",\n");
        if (part)
            rec->score = atof(part);

        // Yeni bölümse, yeni key olarak ekle
        if (currentLeaf->numKeys == 0 || strcmp(currentLeaf->keys[currentLeaf->numKeys - 1], rec->department) != 0)
        {
            // Leaf full ise yenisini oluştur
            if (currentLeaf->numKeys == ORDER - 1)
            {
                Node *newLeaf = createNode(1);
                currentLeaf->next = newLeaf; // leaf nodeları birbirine bağlıyoruz.
                currentLeaf = newLeaf;
            }
            currentLeaf->keys[currentLeaf->numKeys] = strdup(rec->department);
            currentLeaf->records[currentLeaf->numKeys] = rec;
            currentLeaf->numKeys++;
        }
        else
        {
            // Aynı bölüm varsa record'u linked list'e ekle
            Record *head = currentLeaf->records[currentLeaf->numKeys - 1];
            if (rec->score > head->score)
            {
                rec->next = head;
                currentLeaf->records[currentLeaf->numKeys - 1] = rec;
            }
            else
            {
                Record *temp = head;
                while (temp->next && temp->next->score > rec->score)
                {
                    temp = temp->next;
                }
                rec->next = temp->next;
                temp->next = rec;
            }
        }
        count++;
    }

    fclose(fp);
    printf("Bulk loading completed. %d records loaded into leaf nodes.\n", count);

    // Şimdi leaf node'ları yukarıya bağla
    // Level bazında node'ları grup grup alıp üst seviyeleri oluşturacağız

    // 1. adım: leaf node'ları diziye al
    Node *leaves[1000];
    int leafCount = 0;
    currentLeaf = root;
    while (currentLeaf)
    {
        leaves[leafCount++] = currentLeaf;
        currentLeaf = currentLeaf->next;
    }

    // 2. adım: yukarı seviyeleri inşa et
    while (leafCount > 1)
    {
        int newCount = 0;
        Node *parents[1000];
        for (int i = 0; i < leafCount; i += ORDER)
        {
            Node *parent = createNode(0);
            int childCount = 0;
            for (int j = 0; j < ORDER && i + j < leafCount; j++)
            {
                parent->children[childCount] = leaves[i + j];
                if (j > 0)
                {
                    parent->keys[childCount - 1] = strdup(leaves[i + j]->keys[0]);
                }
                childCount++;
            }
            parent->numKeys = childCount - 1;
            parents[newCount++] = parent;
        }
        for (int k = 0; k < newCount; k++)
        {
            leaves[k] = parents[k];
        }
        leafCount = newCount;
    }

    root = leaves[0];
    printf("Tree loading completed.\n");
}
//-------------------------------------------------------------------------------------------------------------------------------------------------------------

// teker teker insertation--------------------------------------------------------------------------------------------------------------------------------------

void addSequential() // sequential ekleme yeri
{
    FILE *fp = fopen("yok_atlas.csv", "r");
    if (!fp)
    {
        printf("File couldn't be opened.\n");
        return;
    }

    char header_line[512];
    if (fgets(header_line, sizeof(header_line), fp) == NULL)
    {
        fclose(fp);
        return;
    }

    Record *rec;
    while ((rec = readOneRecordCsvFile(fp)) != NULL)
    {
        trimSpaces(rec->department);
        trimNewline(rec->department);
        if (rec->department[0] == '\0' || rec->university[0] == '\0' || rec->score == 0.0)
        {
            printf("⚠️ Eksik veya hatalı kayıt atlandı!\n");
            free(rec);
            continue;
        }
        insert(rec->department, rec);
    }
    fclose(fp);
}

//--------------------------------------------------------------------------------------------------------------------------------------------------------------

double calculateUsedMemory()
{
    return (nodeAmount * sizeof(Node)) / 1024.0;
}

// menü
void menu()
{
    int choice;
    while (1)
    {
        printf("\n========= B+ Tree Loader =========\n");
        printf("1 - Sequential Insertion\n");
        printf("2 - Bulk Loading (with external merge sort)\n");
        printf("3 - Search\n");
        printf("Please choose a loading option: ");
        if (scanf("%d", &choice) != 1)
        {
            while (getchar() != '\n')
                ; // input temizle
            printf("Invalid input. Please enter 1 or 2.\n");
            continue;
        }
        if (choice == 1)
        {
            addSequential();

            printf("Sequential insertion completed.\n");
            printf("Number of splits:%d\n", splitAmount);
            printf("Memory usage: %f KB\n", calculateUsedMemory());
            printf("Tree hight: %d\n", getTreeHeight(root));
        }
        else if (choice == 2)
        {
            replacementSort("/home/yaren/Desktop/Dersler bahar 2.sınıf/dom/b+ tree ödevi/B+ tree homework/yok_atlas.csv");
            mergeAllRunFiles();
            bulkLoadFromFinalSorted();

            printf("Bulk loading completed.\n");
            printf("Number of splits:%d\n", splitAmount);
            printf("Memory usage: %f\n", calculateUsedMemory());
            printf("Tree hight: %d\n", getTreeHeight(root));
        }
        else if (choice == 3) // search yeri
        {

            if (root == NULL)
            {
                printf("Tree is empty please first load the tree!\n");
                return;
            }

            char departmanName[100];

            while ((getchar()) != '\n')
                ; // scanf sonrası kalan \n'yi temizle yoksa beklemiyor

            printf("Please enter the department name to search:\n");
            fgets(departmanName, sizeof(departmanName), stdin);
            departmanName[strcspn(departmanName, "\n")] = '\0';
            trimSpaces(departmanName);

            // '\n' varsa sil
            departmanName[strcspn(departmanName, "\n")] = '\0';

            int number;
            printf("Please enter the university rank in that department:\n");
            scanf("%d", &number);
            while (getchar() != '\n')
                ; // stdin temizle

            double time = search(departmanName, number);

            printf("Average seek time : %f\n", time);
        }

        else
        {
            printf("Invalid choice.\n");
        }
    }
}
int main()
{
    menu();
    return 0;
}