# Hướng dẫn Apache Spark: ML với PySpark

Apache Spark được biết đến như một công cụ nhanh, dễ sử dụng và chung để xử lý dữ liệu lớn có các mô-đun tích hợp để xử lý luồng, SQL, Machine Learning (ML) và xử lý đồ thị. Công nghệ này là một kỹ năng cần thiết cho các kỹ sư dữ liệu, nhưng các nhà khoa học dữ liệu cũng có thể hưởng lợi từ việc học Spark khi thực hiện Phân tích dữ liệu khám phá (EDA), trích xuất tính năng và tất nhiên là ML.

## Các bước để xây dựng một chương trình Học máy với PySpark:

+ Bước 1 Hoạt động cơ bản với PySpark
+ Bước 2) Tiền xử lý dữ liệu
+ Bước 3) Xây dựng pipeline xử lý dữ liệu
+ Bước 4) Xây dựng bộ phân loại: logistic
+ Bước 5) Đào tạo và đánh giá mô hình
+ Bước 6) Điều chỉnh siêu tham số
chúng ta sẽ sử dụng tập dữ liệu adult dataset. Mục đích của hướng dẫn này là để học cách sử dụng Pyspark.

### Bước 1 Hoạt động cơ bản với PySpark

Trước hết, bạn cần khởi tạo SQLContext.

![image](https://user-images.githubusercontent.com/64195026/116817432-803c2e00-ab90-11eb-9109-7fd684fd1245.png)

Sau đó, bạn có thể đọc tệp cvs bằng sqlContext.read.csv. Sử dụng  inferSchema được đặt thành True để yêu cầu Spark tự động đoán loại dữ liệu. Theo mặc định, nó chuyển thành False.

![image](https://user-images.githubusercontent.com/64195026/116817447-90540d80-ab90-11eb-967f-a533a5b0a7cf.png)

Hãy xem kiểu dữ liệu

![image](https://user-images.githubusercontent.com/64195026/116817458-9fd35680-ab90-11eb-88b0-026a3337d02b.png)

Bạn có thể xem dữ liệu vs shaow

![image](https://user-images.githubusercontent.com/64195026/116817476-be395200-ab90-11eb-9631-52152973e8b2.png)

Nếu bạn không đặt inderShema thành True, đây là những gì đang xảy ra với type. Có tất cả trong chuỗi.

![image](https://user-images.githubusercontent.com/64195026/116817521-f3de3b00-ab90-11eb-8b9c-c5b2569fad7b.png)

Để chuyển đổi biến liên tục theo đúng định dạng, bạn có thể sử dụng các cột. Bạn có thể sử dụng withColumn để cho Spark biết cột nào sẽ hoạt động chuyển đổi.

![image](https://user-images.githubusercontent.com/64195026/116817556-14a69080-ab91-11eb-93fc-54c01444507d.png)

Select columns Bạn có thể chọn và hiển thị các hàng có lựa chọn và tên của các đặc trưng. Dưới đây, age và fnlwgt được chọn.

![image](https://user-images.githubusercontent.com/64195026/116817573-2ee06e80-ab91-11eb-8b7b-3e755a282a05.png)

Count by group Nếu bạn muốn đếm số lần xuất hiện theo nhóm, bạn có thể xâu chuỗi:
+ groupBy()
+ count()

Trong ví dụ PySpark bên dưới, bạn đếm số hàng theo education level.

![image](https://user-images.githubusercontent.com/64195026/116817587-4c153d00-ab91-11eb-8b30-e9eba71e1718.png)

Describe the data Để nhận thống kê tóm tắt về dữ liệu, bạn có thể sử dụng description():
+ count
+ mean
+ standarddeviation
+ min
+ max

![image](https://user-images.githubusercontent.com/64195026/116817611-6bac6580-ab91-11eb-80a2-223ef01cfcaf.png)

Nếu bạn muốn thống kê tóm tắt chỉ của một cột, hãy thêm tên của cột vào bên trong description().

![image](https://user-images.githubusercontent.com/64195026/116817622-79fa8180-ab91-11eb-80ba-85f7870f68fb.png)

Crosstab computation Trong một số trường hợp, có thể thú vị khi xem các thống kê mô tả giữa hai cột theo cặp. Ví dụ: bạn có thể đếm số người có thu nhập dưới hoặc trên 50k theo trình độ học vấn. Thao tác này được gọi là crosstab.

![image](https://user-images.githubusercontent.com/64195026/116817634-91396f00-ab91-11eb-9f93-6059fb5b632a.png)

Drop column Có hai API trực quan để drop columns:
+ drop(): Drop a column
+ dropna(): Drop NA’s
Bên dưới bạn drop column  education_num

![image](https://user-images.githubusercontent.com/64195026/116817665-b3cb8800-ab91-11eb-8f94-728b02a2bf65.png)

Filter data Bạn có thể sử dụng filter () để áp dụng thống kê mô tả trong một tập hợp con dữ liệu. Ví dụ: bạn có thể đếm số người trên 40 tuổi

![image](https://user-images.githubusercontent.com/64195026/116817690-d52c7400-ab91-11eb-9e4d-52235c8d5ed5.png)

Thống kê mô tả theo nhóm Cuối cùng, bạn có thể nhóm dữ liệu theo nhóm và tính toán các hoạt động thống kê như giá trị trung bình.

![image](https://user-images.githubusercontent.com/64195026/116817701-e5445380-ab91-11eb-9133-7089bbacf1da.png)

## Bước 2 Tiền xử lý dữ liệu
Xử lý dữ liệu là một bước quan trọng trong học máy. Sau khi xóa dữ liệu rác, bạn sẽ có được một số thông tin chi tiết quan trọng.

Ví dụ, bạn biết rằng tuổi không phải là một hàm tuyến tính với thu nhập. Khi còn trẻ, thu nhập của họ thường thấp hơn tuổi trung niên. Sau khi nghỉ hưu, một hộ gia đình sử dụng tiền tiết kiệm của họ, nghĩa là thu nhập giảm. Để chụp mẫu này, bạn có thể thêm square vào đặc trưng tuổi.

Add age square Để thêm một đặc trưng mới, bạn cần:
+ Chọn cột
+ Áp dụng phép biến đổi và thêm nó vào DataFrame

![image](https://user-images.githubusercontent.com/64195026/116817736-158bf200-ab92-11eb-9e45-88bd59126069.png)

Bạn có thể thấy rằng age_square đã được thêm thành công vào khung dữ liệu. Bạn có thể thay đổi thứ tự của các biến với select. Dưới đây, bạn mang theo age_square ngay sau tuổi.

![image](https://user-images.githubusercontent.com/64195026/116817749-22a8e100-ab92-11eb-85fa-8f79798575c1.png)

Loại trừ Holand-Netherlands
Khi một nhóm trong một đặc trưng chỉ có một dữ liệu, nó không mang lại thông tin gì cho mô hình. Ngược lại, nó có thể dẫn đến lỗi trong quá trình cross-validation.

Hãy kiểm tra nguồn gốc của hộ.

![image](https://user-images.githubusercontent.com/64195026/116817766-35bbb100-ab92-11eb-8b35-edb3bd769acd.png)

Đặc trưng native_country chỉ có một hộ gia đình đến từ Hà Lan. Bạn loại trừ nó.

![image](https://user-images.githubusercontent.com/64195026/116817780-4704bd80-ab92-11eb-9a98-5b1da66f69ca.png)

## Bước 3 Xây dựng pipeline xử lý dữ liệu
Tương tự như scikit-learn, Pyspark có API pipeline.

Một pipeline dẫn rất thuận tiện để duy trì cấu trúc của dữ liệu. Bạn đẩy dữ liệu vào pipeline. Bên trong pipeline, các hoạt động khác nhau được thực hiện, đầu ra được sử dụng để cung cấp cho thuật toán.

Ví dụ: một phép biến đổi phổ quát trong học máy bao gồm chuyển đổi một chuỗi thành một one hot encoder, tức là một cột theo nhóm. One hot encoder thường là một ma trận đầy các số 0.

Các bước để biến đổi dữ liệu rất giống với scikit-learn. Bạn cần phải:

+ Lập index chuỗi thành số
+ Tạo một bộ one hot encoder
+ Chuyển đổi dữ liệu
+ Hai API thực hiện công việc: StringIndexer, OneHotEncoder

1. Trước hết, bạn chọn cột chuỗi để lập chỉ mục. InputCol là tên của cột trong tập dữ liệu. OutputCol là tên mới được đặt cho cột được chuyển đổi.
StringIndexer(inputCol="workclass", outputCol="workclass_encoded")

![image](https://user-images.githubusercontent.com/64195026/116817807-6dc2f400-ab92-11eb-949d-4eeb702faaf6.png)

2. Điều chỉnh dữ liệu và biến đổi nó

![image](https://user-images.githubusercontent.com/64195026/116817829-8af7c280-ab92-11eb-9aa6-02e8a4687b5f.png)

3. Tạo các cột news dựa trên nhóm. Ví dụ: nếu có 10 nhóm trong đặc trưng, ma trận mới sẽ có 10 cột, mỗi nhóm một cột.

![image](https://user-images.githubusercontent.com/64195026/116817838-9519c100-ab92-11eb-89e4-c1012f426984.png)

![image](https://user-images.githubusercontent.com/64195026/116817850-a4990a00-ab92-11eb-8a84-53f1530ae68e.png)

![image](https://user-images.githubusercontent.com/64195026/116817858-acf14500-ab92-11eb-9bdd-ad99acab6e19.png)


Xây dựng pipeline Bạn sẽ xây dựng một pipeline để chuyển đổi tất cả các đặc trưng chính xác và thêm chúng vào tập dữ liệu cuối cùng. Pipeline sẽ có bốn hoạt động, nhưng hãy thoải mái thêm bao nhiêu hoạt động tùy thích.
1. Encode dữ liệu phân loại
2. Lập Index label feature
3. Thêm biến liên tục
4. Tập hợp các bước.

Mỗi bước được lưu trữ trong một danh sách có tên các giai đoạn. Danh sách này sẽ cho VectorAssembler biết thao tác nào cần thực hiện bên trong pipeline.

Mã hóa dữ liệu phân loại Bước này cũng giống như ví dụ trên, ngoại trừ việc bạn lặp lại tất cả các đặc trưng phân loại.

![image](https://user-images.githubusercontent.com/64195026/116817902-e0cc6a80-ab92-11eb-9b26-7da4cfcc7c6a.png)

Lập index label feature Spark, giống như nhiều thư viện khác, không chấp nhận các giá trị chuỗi cho nhãn. Bạn chuyển đổi đặc trưng nhãn với StringIndexer và thêm nó vào các giai đoạn danh sách.

![image](https://user-images.githubusercontent.com/64195026/116817937-f9d51b80-ab92-11eb-807d-80764a732424.png)

Thêm biến liên tục InputCols của VectorAssembler là một danh sách các cột. Bạn có thể tạo một danh sách mới chứa tất cả các cột mới.

![image](https://user-images.githubusercontent.com/64195026/116817945-048fb080-ab93-11eb-822b-462f36826e36.png)

Tập hợp các bước Cuối cùng, bạn vượt qua tất cả các bước trong VectorAssembler

![image](https://user-images.githubusercontent.com/64195026/116817962-12ddcc80-ab93-11eb-8f18-8b2de89a0454.png)

Bây giờ tất cả các bước đã sẵn sàng, bạn đẩy dữ liệu vào pipeline.

![image](https://user-images.githubusercontent.com/64195026/116817970-1c673480-ab93-11eb-9d0e-bd0f1c4105b2.png)

Nếu bạn kiểm tra tập dữ liệu mới, bạn có thể thấy rằng nó chứa tất cả các đặc trưng, được chuyển đổi và chưa được chuyển đổi. Bạn chỉ quan tâm đến nhãn mới và các đặc trưng.

![image](https://user-images.githubusercontent.com/64195026/116817986-3739a900-ab93-11eb-873b-f853945b75c7.png)

## Bước 4 Xây dựng bộ phân loại: logistic

Để tính toán nhanh hơn, bạn chuyển đổi mô hình thành DataFrame.
Bạn cần chọn nhãn mới và các đặc trưng từ mô hình bằng cách sử dụng map.

![image](https://user-images.githubusercontent.com/64195026/116818000-50425a00-ab93-11eb-84e3-4f4df20b8716.png)

Bạn đã sẵn sàng tạo dữ liệu train dưới dạng DataFrame. Sử dụng sqlContext

![image](https://user-images.githubusercontent.com/64195026/116818012-5df7df80-ab93-11eb-9e03-ea97561da5a2.png)

Kiểm tra hàng thứ hai

![image](https://user-images.githubusercontent.com/64195026/116818027-7405a000-ab93-11eb-93d8-e8c120b0be0f.png)

Tạo train/test set Bạn chia tập dữ liệu 80/20 với randomSplit.

![image](https://user-images.githubusercontent.com/64195026/116818049-94355f00-ab93-11eb-9c50-33fae03abff2.png)

Hãy đếm xem có bao nhiêu người có thu nhập dưới / trên 50k trong cả tập huấn luyện và kiểm tra.

![image](https://user-images.githubusercontent.com/64195026/116818071-af07d380-ab93-11eb-8fed-366c0e2ab3c5.png)

Xây dựng bộ hồi quy logistic
Cuối cùng nhưng không kém phần quan trọng, bạn có thể xây dựng bộ phân loại. Pyspark có một API gọi là LogisticRegression để thực hiện hồi quy logistic.

Bạn khởi tạo lr bằng cách chỉ ra cột nhãn và các cột đặc trưng. Đặt tối đa 10 lần lặp và thêm thông số chính quy hóa với giá trị 0,3. Lưu ý rằng trong phần tiếp theo, bạn sẽ sử dụng xác thực chéo với lưới tham số để điều chỉnh mô hình.

![image](https://user-images.githubusercontent.com/64195026/116818090-be871c80-ab93-11eb-899d-f7da3e698c9d.png)

# Bạn có thể xem các hệ số từ hồi quy

![image](https://user-images.githubusercontent.com/64195026/116818103-c9da4800-ab93-11eb-85b3-1ddb7d521247.png)

## Bước 5 Đào tạo và đánh giá mô hình
Để tạo dự đoán cho bộ thử Bạn cần phải xem chỉ số độ chính xác để xem mô hình hoạt động tốt (hoặc xấu) như thế nào.nghiệm của bạn. Bạn có thể sử dụng linearModel với transform() trên test_data.

![image](https://user-images.githubusercontent.com/64195026/116818119-e0809f00-ab93-11eb-928b-f32bc1769cbb.png)
Bạn có thể in các phần tử trong dự đoán

![image](https://user-images.githubusercontent.com/64195026/116818122-e6768000-ab93-11eb-9764-905ab415be1c.png)

Bạn quan tâm đến nhãn, dự đoán và xác suất

![image](https://user-images.githubusercontent.com/64195026/116818140-f2fad880-ab93-11eb-86c5-b44295b6f944.png)

Đánh giá mô hình Bạn cần phải xem chỉ số độ chính xác để xem mô hình hoạt động tốt (hoặc xấu) như thế nào. Hiện tại, không có API nào để tính toán độ chính xác trong Spark. Giá trị mặc định là ROC (receiver operating characteristic curve).

Trước khi bạn xem xét ROC, hãy xây dựng thước đo độ chính xác. Thước đo độ chính xác là tổng của dự đoán đúng trên tổng số quan sát.

Bạn tạo một DataFrame với nhãn và dự đoán

![image](https://user-images.githubusercontent.com/64195026/116818163-19207880-ab94-11eb-97ef-2b5924a55112.png)

Bạn có thể kiểm tra số lượng lớp trong nhãn và dự đoán

![image](https://user-images.githubusercontent.com/64195026/116818169-250c3a80-ab94-11eb-82af-347ad962383b.png)

Ví dụ, trong tập thử nghiệm, có 1578 hộ gia đình có thu nhập trên 50k và 5021 hộ dưới. Tuy nhiên, phân loại dự đoán 617 hộ gia đình có thu nhập trên 50 nghìn.

Bạn có thể tính độ chính xác bằng cách tính số lượng khi nhãn được phân loại chính xác trên tổng số hàng.

![image](https://user-images.githubusercontent.com/64195026/116818180-348b8380-ab94-11eb-8d65-2678caf96364.png)

Bạn có thể kết hợp mọi thứ lại với nhau và viết một hàm để tính độ chính xác.

![image](https://user-images.githubusercontent.com/64195026/116818185-3fdeaf00-ab94-11eb-8a09-76b96a6f2a5c.png)

ROC metrics
Mô-đun BinaryClassificationEvaluator bao gồm các biện pháp ROC. Receiver Operating Characteristic curve là một công cụ phổ biến khác được sử dụng với phân loại nhị phân. Nó rất giống với precision/recall nhưng thay vì vẽ biểu đồ precision so với recall. ROC cho thấy tỷ lệ dương tính thực sự (tức là recall) so với tỷ lệ dương tính giả.Tỷ lệ dương tính giả là tỷ lệ các trường hợp tiêu cực được phân loại không chính xác là dương tính. Tỷ lệ âm thực sự còn được gọi là độ đặc hiệu. Do đó, đường cong ROC biểu thị độ nhạy (recall) so với 1 – độ đặc hiệu.

![image](https://user-images.githubusercontent.com/64195026/116818190-4c630780-ab94-11eb-9623-5d414d9dbd89.png)
![image](https://user-images.githubusercontent.com/64195026/116818207-67357c00-ab94-11eb-81d0-f7ef00817f55.png)

## Bước 6 Điều chỉnh siêu tham số

Cuối cùng nhưng không kém phần quan trọng, bạn có thể điều chỉnh các siêu tham số.
Để giảm thời gian tính toán, bạn chỉ điều chỉnh tham số chính quy chỉ với hai giá trị.

![image](https://user-images.githubusercontent.com/64195026/116818222-7b797900-ab94-11eb-8783-f3139bdaf8b4.png)

Cuối cùng, bạn đánh giá mô hình bằng cách sử dụng phương pháp cross valiation.

![image](https://user-images.githubusercontent.com/64195026/116818231-86340e00-ab94-11eb-9740-5773a1f4c5c9.png)

Thời gian đào tạo mô hình: 978.807 giây
Siêu tham số đo chính quy tốt nhất là 0,01, với độ chính xác 85,316 phần trăm.

![image](https://user-images.githubusercontent.com/64195026/116818246-96e48400-ab94-11eb-8665-24bbca7b8dc5.png)

Bạn có thể loại trừ tham số được đề xuất bằng cách chaining cvModel.bestModel với extractParamMap().

{Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='aggregationDepth', doc='suggested depth for treeAggregate (>= 2)'): 2,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='elasticNetParam', doc='the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty'): 0.0,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='family', doc='The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial.'): 'auto',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='featuresCol', doc='features column name'): 'features',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='fitIntercept', doc='whether to fit an intercept term'): True,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='labelCol', doc='label column name'): 'label',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='maxIter', doc='maximum number of iterations (>= 0)'): 10,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='predictionCol', doc='prediction column name'): 'prediction',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='probabilityCol', doc='Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities'): 'probability',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='rawPredictionCol', doc='raw prediction (a.k.a. confidence) column name'): 'rawPrediction',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='regParam', doc='regularization parameter (>= 0)'): 0.01,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='standardization', doc='whether to standardize the training features before fitting the model'): True,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='threshold', doc='threshold in binary classification prediction, in range [0, 1]'): 0.5,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='tol', doc='the convergence tolerance for iterative algorithms (>= 0)'): 1e-06}

















