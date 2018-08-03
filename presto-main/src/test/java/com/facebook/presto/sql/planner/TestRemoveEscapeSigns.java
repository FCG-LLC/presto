package com.facebook.presto.sql.planner;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestRemoveEscapeSigns
{
    @Test
    public void removesSingleEscapeChar()
    {
        char escapeChar = 'x';
        String string = "asdfxasdf";

        String outputString = DomainTranslator.Visitor.removeEscapeSigns(string, escapeChar);

        assertEquals(outputString, "asdfasdf");
    }

    @Test
    public void leavesStringAsItIsWhenNoEscaping()
    {
        char escapeChar = '.';

        String string = "asdffsda";

        String outputString = DomainTranslator.Visitor.removeEscapeSigns(string, escapeChar);

        assertEquals(outputString, string);
    }

    @Test
    public void reducesMultipleEscapeChars()
    {
        char escapeChar = 'A';
        String string = "asdfAAxaAAsdf";

        String outputString = DomainTranslator.Visitor.removeEscapeSigns(string, escapeChar);

        assertEquals(outputString, "asdfAxaAsdf");
    }

    @Test
    public void leavesSingleLastEscapeChar()
    {
        char escapeChar = 'A';
        String string = "asdfxasdfA";

        String outputString = DomainTranslator.Visitor.removeEscapeSigns(string, escapeChar);

        assertEquals(outputString, string);
    }

    @Test
    public void reducesLastDoubleEscapeChar()
    {
        char escapeChar = 'A';
        String string = "asdfxasdfAA";

        String outputString = DomainTranslator.Visitor.removeEscapeSigns(string, escapeChar);

        assertEquals(outputString, "asdfxasdfA");
    }
}
