
package it.unipi.hadoop;
import java.text.Normalizer;
import java.util.regex.Pattern;
public class CharacterProcessor {
 
    public static char processCharacter(char c) {
        // Normalizza il carattere per rimuovere le accentazioni
        String normalized = Normalizer.normalize(String.valueOf(c), Normalizer.Form.NFD);
        Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
        char baseChar = pattern.matcher(normalized).replaceAll("").charAt(0);
 
        // Verifica se il carattere base è una lettera dell'alfabeto
        if (Character.isLetter(baseChar)) {
            // Se il carattere base è maiuscolo, lo trasforma in minuscolo
            if (Character.isUpperCase(baseChar)) {
                return Character.toLowerCase(baseChar);
            }
            // Se il carattere base è minuscolo, lo ritorna
            return baseChar;
        } else {
            // Ritorna 0 per indicare un carattere non valido
            return 0;
        }
    }
}
 