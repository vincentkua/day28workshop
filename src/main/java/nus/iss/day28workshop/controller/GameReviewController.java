package nus.iss.day28workshop.controller;

import java.util.Date;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import nus.iss.day28workshop.repository.GameReviewRepository;

@RestController
public class GameReviewController {

    @Autowired
    GameReviewRepository gameReviewRepository;

    @GetMapping(value = "/game/{gameid}/reviews", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getGameReview(@PathVariable("gameid") Integer gameid) {

        Document gameandreviews = gameReviewRepository.findandembeded(gameid);

        if (gameandreviews != null) {
            gameandreviews.put("timestamp", new Date());
            System.out.println(gameandreviews.toJson());
            return ResponseEntity.status(HttpStatus.OK).body(gameandreviews.toJson());
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Game Not Found...");
        }
    }

    // find distinct and do loop for all to all games...
    @GetMapping(value = "/games/highest/{gameid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getGameHighest(@PathVariable("gameid") Integer gameid) {
        
        Document highestreview = gameReviewRepository.findHighestbyId(gameid);

        if (highestreview != null) {
            highestreview.put("timestamp", new Date());
            System.out.println(highestreview.toJson());
            return ResponseEntity.status(HttpStatus.OK).body(highestreview.toJson());
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Review Not Found...");
        }

    }



}
