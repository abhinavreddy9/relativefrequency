package com.wiki.freq.bean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordBean implements Writable,WritableComparable<WordBean> {

    private Text word;
    private Text neighborWord;

    public WordBean(Text word, Text neighbor) {
        this.word = word;
        this.neighborWord = neighbor;
    }

    public WordBean(String word, String neighbor) {
        this(new Text(word),new Text(neighbor));
    }

    public WordBean() {
        this.word = new Text();
        this.neighborWord = new Text();
    }

    @Override
    public int compareTo(WordBean other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighborWord.toString().equals("*")){
            return -1;
        }else if(other.getNeighborWord().toString().equals("*")){
            return 1;
        }
        return this.neighborWord.compareTo(other.getNeighborWord());
    }

    public static WordBean read(DataInput in) throws IOException {
        WordBean wordPair = new WordBean();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighborWord.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighborWord.readFields(in);
    }

    @Override
    public String toString() {
          return (word + " " + neighborWord); 
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordBean wordPair = (WordBean) o;

        if (neighborWord != null ? !neighborWord.equals(wordPair.neighborWord) : wordPair.neighborWord != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 89 * result + (neighborWord != null ? neighborWord.hashCode() : 0);
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighborWord(String neighborWord){
        this.neighborWord.set(neighborWord);
    }

    public Text getWord() {
        return word;
    }

    public Text getNeighborWord() {
        return neighborWord;
    }
}
