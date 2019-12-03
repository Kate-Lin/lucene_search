package com.text.search_lucene;
import org.apache.lucene.store.RAMDirectory;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileSystems;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

//import com.chenlb.mmseg4j.Dictionary;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
//import org.apache.lucene.analysis.cn.smart.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.Term;
//import org.apache.lucene.queryparser.surround.parser.QueryParser;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;


public class App {

    Connection conn = null;
    static {
        try {
            // 加载MySql的驱动类
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
        }
    }

    public static Connection createConnection() {

        String url = "jdbc:mysql://localhost:3306/thunder";
        String username = "root";
        String password = "990207";
        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
            se.printStackTrace();
        }

        return con;
    }

    
    public Connection getConnection() {

    return this.conn;
  
   }
 
    public static void main(String args[])throws Exception {
        Connection conn = createConnection();
        String sql = "SELECT * FROM hupu";  
        //IndexWriter indexWriter = null;  
        Path path = Paths.get("D:\\lucene_index");
        Directory dirWrite = FSDirectory.open(path); 
        //RAMDirectory dir = new RAMDirectory();
            //indexFile = new File(searchDir); 
        Analyzer standardAnalyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(standardAnalyzer);
        iwc.setOpenMode(OpenMode.CREATE);
        IndexWriter indexWriter = new IndexWriter(dirWrite, iwc);
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Document doc = new Document();
            doc.add(new TextField("news", rs.getString(1),Store.YES));
            doc.add(new TextField("author", rs.getString(2), Store.YES));
            doc.add(new StringField("url", rs.getString(3), Store.YES));
            //System.out.println(doc);
            indexWriter.addDocument(doc);
            //System.out.println(indexWriter);
        }
        rs.close();
        indexWriter.close();
        dirWrite.close();
        System.out.println("index built succeed");
        System.out.println("Start searching.");
        //System.out.println(dirWrite);
        Path path_ = Paths.get("D:\\lucene_index");
        Directory direct = FSDirectory.open(path_);
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(direct));
        QueryParser parser = new QueryParser("news", standardAnalyzer);
        //parser.setDefaultOperator(QueryParser.OR_OPERATOR);
        Query query = parser.parse("雷");
        System.out.println(query);
        TopDocs hits = searcher.search(query,20);
        System.out.println(hits.totalHits);
        for (ScoreDoc scoreDoc:hits.scoreDocs){
            Document doc=searcher.doc(scoreDoc.doc);
            System.out.println(doc.get("news")+ " "+ doc.get("author") + " "+ doc.get("url"));
        }
    }

}