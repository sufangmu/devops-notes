#include <stdio.h>
#include <stdlib.h>
#define MAXSIZE 20 // 存储空间初始分配量
#define OK 1
#define ERROR 0
#define TRUE 1
#define FALSE 0

typedef int Status;

typedef int ElemType;
typedef struct
{
	ElemType data[MAXSIZE]; // 数组存储元素，最大值为MAXSIZE
	int length;				// 线性表当前长度
} SqList;

// 初始化线性表
Status InitList(SqList *L)
{
	L->length = 0;
	return OK;
}

// 返回L中数据元素个数
int ListLength(SqList L)
{
	return L.length;
}

// 判断列表是否为空
// 若L为空表，则返回TRUE，否则返回FALSE
Status ListEmpty(SqList L)
{
	if (L.length == 0)
		return TRUE;
	else
		return FALSE;
}

// 清空元素
Status ClearList(SqList *L)
{ 
    L->length=0;
    return OK;
}

// 遍历元素
// 依次对L的每个数据元素输出
Status ListTraverse(SqList L)
{
	int i;
	for (i = 0; i < L.length; i++)
		printf("%d ", L.data[i]);
	printf("\n");
	return OK;
}

// 获取元素
// Status 是函数类型，其值时函数结果返回状态码，如OK等，
// 初始条件：顺序线性表L已存在，1<=i<=ListLength(L)
// 操 结果：用e返回L中第i个元素的值
Status GetElem(SqList L, int i, ElemType *e)
{
	if (L.length == 0 || i < 1 || i > L.length)
		return ERROR;
	*e = L.data[i - 1];
	return OK;
}

// 插入操作
// 思路：
// 1. 如果插入的位置不合理，抛出异常
// 2. 如果线性表长度大于等于数组长度，则抛出异常或动态增加容量
// 3. 从最后一个元素开始向前遍历到第i个位置，分别将他们都向后移动一个位置
// 4. 将要插入的元素填入位置i处
// 5. 表长加1
Status ListInsert(SqList *L, int i, ElemType e)
{
	int k;
	if (L->length == MAXSIZE) // 顺序线性表已满
	{
		return ERROR;
	}
	if (i < 1 || i > L->length + 1) // i不在范围内
		return ERROR;
	if (i <= L->length)							 // 若插入的数据位置不在表尾
		for (k = L->length - 1; k >= i - 1; k--) // 将要插入位置后数据元素向后移动一位
			L->data[k + 1] = L->data[k];
	L->data[i - 1] = e; // 将新元素插入
	L->length++;
	return OK;
}

// 删除元素
// 删除L的第i个数据元素，并用e返回其值，L的长度减1 */
Status ListDelete(SqList *L, int i, ElemType *e)
{
	int k;
	if (L->length == 0) /* 线性表为空 */
		return ERROR;
	if (i < 1 || i > L->length) /* 删除位置不正确 */
		return ERROR;
	*e = L->data[i - 1];
	if (i < L->length) /* 如果删除不是最后位置 */
	{
		for (k = i; k < L->length; k++) /* 将删除位置后继元素前移 */
			L->data[k - 1] = L->data[k];
	}
	L->length--;
	return OK;
}

// 获取元素位置
// 返回L中第1个与e满足关系的数据元素的位序
// 若这样的数据元素不存在，则返回值为0
int LocateElem(SqList L, ElemType e)
{
	int i;
	if (L.length == 0)
		return 0;
	for (i = 0; i < L.length; i++)
	{
		if (L.data[i] == e)
			break;
	}
	if (i >= L.length)
		return 0;
	return i + 1;
}

int main()
{
	SqList ListA;
	Status status;
	// 初始化
	InitList(&ListA);
	// 当前是否为空
	status = ListEmpty(ListA);
	printf("初始化后，ListA是否空：status=%d(1:是 0:否)\n", status); // 初始化后，ListA是否空：status=1(1:是 0:否)
	printf("初始化后，ListA长度：ListA.length=%d\n", ListA.length);	 // 初始化后，ListA长度：ListA.length=0
	// 添加元素,在ListA的表头依次插入1-5
	int j;
	for (j = 1; j <= 5; j++)
		ListInsert(&ListA, 1, j);
	status = ListEmpty(ListA);
	printf("在表头添加元素后，ListA是否空：status=%d(1:是 0:否)\n", status); // 初添加元素后，ListA是否空：status=0(1:是 0:否)
	printf("在表头添加元素后，ListA长度：ListA.length=%d\n", ListA.length);	 // 添加元素后，ListA长度：ListA.length=5
	printf("在表头添加元素后, ListA.data=");								 // 添加元素后：ListA.data=5 4 3 2 1
	ListTraverse(ListA);
	printf("在表头添加元素后, ListLength=%d\n", ListLength(ListA));

	// 获取指定位置元素
	ElemType e;
	GetElem(ListA, 2, &e);
	printf("第2个元素的值为：%d\n", e);  // 第2个元素的值为：4

	ListInsert(&ListA, 1, 0); // 在表头添加元素0
	printf("在表头添加元素0后, ListA.data=");  // 在表头添加元素0后, ListA.data=0 5 4 3 2 1
	ListTraverse(ListA);

	// 删除元素
	ListDelete(&ListA, 5, &e); // 删除第5个数据
	printf("删除第5个的元素值为：%d\n", e);  // 删除第5个的元素值为：2
	printf("删除第5个的元素后，ListA.data=");  // 删除第5个的元素后，ListA.data=0 5 4 3 1
	ListTraverse(ListA);

	// 获取元素的位置
	int n;
	n = LocateElem(ListA, 5);  // 获取元素5所在位置
	printf("元素5所在的位置为：%d\n", n); // 元素5所在的位置为：2

	// 清空元素
	ClearList(&ListA);
	printf("清空元素后，ListA是否空：status=%d(1:是 0:否)\n", status);  // 清空元素后，ListA是否空：status=0(1:是 0:否)
	printf("清空元素后，ListA长度：ListA.length=%d\n", ListA.length);	 // 清空元素后，ListA长度：ListA.length=0


	return 0;
}