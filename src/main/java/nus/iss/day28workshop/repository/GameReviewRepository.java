package nus.iss.day28workshop.repository;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
public class GameReviewRepository {

    @Autowired
    MongoTemplate mongoTemplate;

    //Task A
    public Document findandembeded(Integer gameid) {
        MatchOperation matchId = Aggregation.match(Criteria.where("gid").is(gameid));
        LookupOperation lookupreview = Aggregation.lookup("comment", "gid", "gid", "reviews");
        Aggregation pipeline = Aggregation.newAggregation(matchId, lookupreview);
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "game", Document.class);

        if(results.getMappedResults().size() == 0){
            System.out.println("nothing was found...");
            return null;
        } else{
            System.out.println("OK here your go");
            System.out.println(results.getMappedResults().size() + " Games are found");
            List<Document> gamewithreview = results.getMappedResults();
            Document the1stgamefound = gamewithreview.get(0); // Take the 1st Document in the List only
            return the1stgamefound;
        }
    }


    // Task B
    public Document findHighestbyId(Integer gameid){
        MatchOperation matchId = Aggregation.match(Criteria.where("gid").is(gameid));
        ProjectionOperation projectField = Aggregation.project("gid", "user", "rating" , "c_text").andExclude("_id");
        SortOperation sortByRating = Aggregation.sort(
                Sort.by(Direction.DESC, "rating"));
        LimitOperation limitToOne = Aggregation.limit(1);
        Aggregation pipeline = Aggregation.newAggregation(matchId, projectField, sortByRating ,limitToOne);
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "comment", Document.class);

        if(results.getMappedResults().size() == 0){
            System.out.println("nothing was found...");
            return null;
        } else{
            System.out.println("OK here your go");
            System.out.println(results.getMappedResults().size() + " Games are found");
            List<Document> highestreview = results.getMappedResults();
            Document the1streview = highestreview.get(0); // Take the 1st Document in the List only
            return the1streview;
        }

    }


    //giveup........
    public void findHighestAll(){
        GroupOperation grouprating = Aggregation.group("gid")
        .max("rating").as("maxrating");
        // .push(new Document("user", "$user").append("rating", "$rating")).as("reviews"); //exceed memory !!!!!
        Aggregation pipeline = Aggregation.newAggregation(grouprating);
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "comment", Document.class);
        for (Document doc : results) {
            System.out.println(doc);
        }

    }



    //Other Sample ================================================================================================

    public void findGame(Integer gameid) {
        Query query = Query.query(Criteria.where("gid").is(gameid));
        List<Document> games = mongoTemplate.find(query, Document.class, "game");
        System.out.println(games);
    }

    public void findReview(Integer gameid) {
        Query query = Query.query(Criteria.where("gid").is(gameid));
        List<Document> games = mongoTemplate.find(query, Document.class, "comment");
        System.out.println(games);
    }

    public void findCommentMatch(Integer gameid) {
        // stages
        MatchOperation matchId = Aggregation.match(Criteria.where("gid").is(gameid));
        ProjectionOperation projectField = Aggregation.project("gid", "user", "rating").andExclude("_id");
        SortOperation sortByRating = Aggregation.sort(
                Sort.by(Direction.ASC, "rating"));

        // Create Pipeline and execute
        Aggregation pipeline = Aggregation.newAggregation(matchId, projectField, sortByRating);
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "comment", Document.class);

        for (Document doc : results) {
            System.out.println(doc.toJson());
        }

    }

    public void findCommentandGroup(Integer gameid) {
        MatchOperation matchId = Aggregation.match(
                Criteria.where("gid").is(gameid));
        GroupOperation grouprating = Aggregation.group("rating")
                .count().as("count")
                .push("user").as("commentby");
        ;
        Aggregation pipeline = Aggregation.newAggregation(matchId, grouprating);
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "comment", Document.class);
        for (Document doc : results) {
            System.out.println(doc.toJson());
        }
    }

    // Unwind Operation Sample
    // UnwindOperation unwindsample = Aggregation.unwind("category");

}
