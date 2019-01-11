
package edu.sjsu.cs185C;

public class Constants 
{
    static final String Quorum = "localhost";
    static final String Port = "5181";

    static final String CF1 = "info";
    static final String CF2 = "rating";
    static final String CF3 = "catalog";

    static final int FieldCount = 44;
    static final int RatingFieldIndex = 5;
    static final int CatalogFieldStartIndex = 16;


    static final String[] QualNames = {"fn","tid","title","wordsInTitle","url","imdbRating","ratingCount","duration","year","type","nrOfWins","nrOfNominations","nrOfPhotos","nrOfNewsArticles","nrOfUserReviews","nrOfGenre","Action","Adult","Adventure","Animation","Biography","Comedy","Crime","Documentary","Drama","Family","Fantasy","FilmNoir","GameShow","History","Horror","Music","Musical","Mystery","News","RealityTV","Romance","SciFi","Short","Sport","TalkShow","Thriller","War","Western"};
    static final String[] CfNames = {"","","info","","info","rating","rating","info","info","","rating","rating","","","","","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog","catalog"};

}
