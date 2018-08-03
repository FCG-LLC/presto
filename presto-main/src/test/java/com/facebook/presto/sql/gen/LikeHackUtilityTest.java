package com.facebook.presto.sql.gen;

import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.LikeHackUtility.StringMetaCharacter.CONTAINS;
import static com.facebook.presto.sql.gen.LikeHackUtility.StringMetaCharacter.ENDS_WITH;
import static com.facebook.presto.sql.gen.LikeHackUtility.StringMetaCharacter.STARTS_WITH;
import static org.testng.Assert.assertEquals;

public class LikeHackUtilityTest
{
    public static class SplitLikeStrings
    {
        LikeHackUtility utility = new LikeHackUtility();

        @Test
        public void returnsSameStringWhenNoWildcard()
        {
            String input = "asd";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), input);
        }

        @Test
        public void splitsIntoStartEndWhenWildcardInside()
        {
            String input = "asd%dsa";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 2);
            assertEquals(result.get(0), STARTS_WITH.getCharacter() + "asd");
            assertEquals(result.get(1), ENDS_WITH.getCharacter() + "dsa");
        }

        @Test
        public void returnsEndWhenWildcardOnBeginning()
        {
            String input = "%asd";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), ENDS_WITH.getCharacter() + "asd");
        }

        @Test
        public void returnsStartWhenWildcardOnBeginning()
        {
            String input = "asd%";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), STARTS_WITH.getCharacter() + "asd");
        }

        @Test
        public void splitsIntoMoreThanTwoStrings()
        {
            String input = "asd%dsa%qqq%";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 3);
            assertEquals(result.get(0), STARTS_WITH.getCharacter() + "asd");
            assertEquals(result.get(1), CONTAINS.getCharacter() + "dsa");
            assertEquals(result.get(2), CONTAINS.getCharacter() + "qqq");
        }

        @Test
        public void ignoresConsequentWildcards()
        {
            String input = "asd%dsa%%%%%qqq%";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 3);
            assertEquals(result.get(0), STARTS_WITH.getCharacter() + "asd");
            assertEquals(result.get(1), CONTAINS.getCharacter() + "dsa");
            assertEquals(result.get(2), CONTAINS.getCharacter() + "qqq");
        }

        @Test
        public void ignoresEscapedWildcards()
        {
            String input = "asdx%";
            List<String> result = utility.splitLikeStrings(input, Optional.of('x'));
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), "asdx%");
        }

        @Test
        public void ignoresEscapedWildcardsByBackslash()
        {
            String input = "asd\\%xxx";
            List<String> result = utility.splitLikeStrings(input, Optional.of('\\'));
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), "asd\\%xxx");
        }

        @Test
        public void ignoresEscapedWildcardsByBackslashWhileSplitting()
        {
            String input = "asd\\%%xxx";
            List<String> result = utility.splitLikeStrings(input, Optional.of('\\'));
            assertEquals(result.size(), 2);
            assertEquals(result.get(0), STARTS_WITH.getCharacter() + "asd\\%");
            assertEquals(result.get(1), ENDS_WITH.getCharacter() + "xxx");
        }

        @Test
        public void wildcardStringIsTransformedToContainsAll()
        {
            String input = "%%%";
            List<String> result = utility.splitLikeStrings(input, Optional.empty());
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), String.valueOf(CONTAINS.getCharacter()));
        }
    }
}
