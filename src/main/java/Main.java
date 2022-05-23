import com.mongodb.client.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;


import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;


public class Main {
    private static String csvFile = "C:\\Users\\Павел\\Documents\\Skillbox\\java_basics\\18_NoSQL\\MongoDBcsv\\src\\main\\data\\mongo.csv";
    public static int age;
    public static MongoCollection<Document> collection;
    public static BsonDocument query;

    public static void main(String[] args) throws IOException {
        //MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );
        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");

        MongoDatabase database = mongoClient.getDatabase("local");

        // Создаем коллекцию
        collection = database.getCollection("TestSkillDemo");

        // Удалим из нее все документы
        collection.drop();

        MongoCollection<Document> students = parseCSV();

        allStudents(students);
        older40years(students);
        oneYoungStudent(students);
        corsesOlderstudent(students);


//        // Создадим первый документ
//        Document firstDocument = new Document()
//                .append("Type", 1)
//                .append("Description", "Это наш первый документ в MongoDB")
//                .append("Author", "Я")
//                .append("Time", new SimpleDateFormat().format(new Date()));
//
//        // Вложенный объект
//        Document nesteObject = new Document()
//                .append("Course", "NoSQL Базы Данных")
//                .append("Author", "Mike Ovchinnikov");
//
//        firstDocument.append("skillbox",nesteObject);
//
//        // Вставляем документ в коллекцию
//        collection.insertOne(firstDocument);

//        collection.find().forEach((Consumer<Document>) document ->
//                System.out.println("Наш первый документ:\n" + document));
//
//        // Используем JSON-синтаксис для создания объекта
//        Document secondDocument = Document.parse(
//                "{Type: 2, Description:\"Мы создали и нашли этот документ с помощью JSON-синтаксиса\"}"
//        );
//        collection.insertOne(secondDocument);
//
//        // Используем JSON-синтаксис для написания запроса (выбираем документы с Type=2)
//        BsonDocument query = BsonDocument.parse("{Type: {$eq: 2}}");
//        collection.find(query).forEach((Consumer<Document>) document -> {
//            System.out.println("Наш второй документ:\n" + document);
//        });
    }


    public static MongoCollection<Document> parseCSV() throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);

        //Доступ по столбцам
        for (CSVRecord csvRecord : csvParser) {
            String name = csvRecord.get(0);
            int age = Integer.parseInt(csvRecord.get(1));
            List<String> list = Collections.singletonList(csvRecord.get(2));

            createMongo(name, age, list);
        }
        return null;
    }

    public static void createMongo(String name, int age, List list) {
        // Создадим первый документ
        Document firstDocument = new Document()
                .append("name", name)
                .append("age", age)
                .append("CourseList", list);

        // Вставляем документ в коллекцию
        collection.insertOne(firstDocument);
    }

    public static void allStudents(MongoCollection<Document> students) {
        System.out.println("Всего студентов: " + collection.countDocuments());
    }

    public static void older40years(MongoCollection<Document> students) {
        System.out.println("Студенты старше 40 лет: " + collection.countDocuments(BsonDocument.parse("{age: {$gt: 40}}")));
    }

    public static void oneYoungStudent(MongoCollection<Document> students) {
        query = BsonDocument.parse("{age: 1}");
        System.out.println("Имя самого молодого студента: ");
        collection.find().sort(query).limit(1).forEach((Consumer<Document>) document -> {
            age = (int) document.get("age");
        });
        collection.find().sort(query).forEach((Consumer<Document>) document -> {
            if (document.getInteger("age") == age) {
                System.out.println(document.getString("name") + " возраст: " + document.get("age"));
            }
        });
    }

    public static void corsesOlderstudent(MongoCollection<Document> students) {
        query = BsonDocument.parse("{age: -1}");
        System.out.println("Имя самого старого студента: ");
        collection.find().sort(query).limit(1).forEach((Consumer<Document>) document -> {
            age = document.getInteger("age");
        });
        collection.find().sort(query).forEach((Consumer<Document>) document -> {
            if (document.getInteger("age") == age) {
                System.out.println(document.getString("name") + " возраст: " + document.getInteger("age") +
                        " Список курсов: " + document.get("CourseList"));
            }
        });


    }


}
//db.books.insertMany([
//        {"book": "Три товарища", "author":"Remark", "year": 1944},
//        {"book": "Жизнь взаймы", "author":"Remark", "year": 1954},
//        {"book": "Триумфальная арка", "author":"Remark", "year": 1947},
//        {"book": "Норвежский лес", "author":"Мураками", "year": 1985},
//        {"book": "1Q84", "author":"Мураками", "year": 2009}
//        ])