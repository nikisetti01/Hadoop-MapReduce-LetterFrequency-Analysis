package it.unipi.hadoop;

import java.text.Normalizer;
import java.util.regex.Pattern;

public class CharacterProcessor {

    /**
     * Processes a character by normalizing it to remove accents and converting to lowercase.
     *
     * @param c the character to process
     * @return the processed character, or 0 if the character is not valid
     */
    
    public static char processCharacter(char c) {
        // Normalize the character to decompose accentuated characters
        String normalized = Normalizer.normalize(String.valueOf(c), Normalizer.Form.NFD);

        // Compile a pattern to match diacritical marks
        Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");

        // Remove diacritical marks from the normalized string and get the base character
        char baseChar = pattern.matcher(normalized).replaceAll("").charAt(0);

        // Check if the base character is a letter
        if (Character.isLetter(baseChar)) {
            // If the base character is uppercase, convert it to lowercase
            if (Character.isUpperCase(baseChar)) {
                return Character.toLowerCase(baseChar);
            }
            // If the base character is already lowercase, return it
            return baseChar;
        } else {
            // Return 0 to indicate an invalid character
            return 0;
        }
    }
}
