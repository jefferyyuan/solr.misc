package org.codeexample.jeffery.solr;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieLongField;


public class RoundLongField extends TrieLongField {
  
  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
//    switch (type) {
//      case LONG:
//        value = String.valueOf(roundNumber(Long.parseLong(value.toString())));
//        break;
//      default:
//        break;
//    }
    
    value = String.valueOf(roundNumber(Long.parseLong(value.toString())));
    return super.createField(field, value, boost);
  }
  
  /**
   * This represents value between 0, 1k, 1M, 1G, 1T, 1024T, each block will be
   * rounded to 1024 number, for example: the first block represent value
   * [0-1014],the second block represent value between [1024+1 - 1024*1024],
   * there is totally 1024*1024 -1 values, value between [1024 - 1024+ 1023]
   */
  private long[] partitions = {0, 1024L, 1024 * 1024L, 1024 * 1024 * 1024L,
      1024 * 1024 * 1024L * 1024L, 1024 * 1024 * 1024L * 1024L * 1024L};
  
  /**
   * 
   * This will round a value for a value 1025, i = 2, means it is in range
   * [1024, 1024*1024]
   * 
   * @param input
   * @return
   */
  private long roundNumber(long input) {
    if (input < partitions[0] || input > partitions[partitions.length - 1]) throw new IllegalArgumentException(
        "Input: " + input + " is not valid. Must be in range [ "
            + partitions[0] + ", " + partitions[partitions.length - 1]);
    int i = 0;
    // determine which range the input value is in
    for (; i < partitions.length && input > partitions[i]; i++)
      ;
    // for a value 1025, i = 2, means it is in range [1024, 1024*1024]
    long result;
    if (i == 0 || i == 1) {
      result = input;
    } else {
      // now i point to the start range.
      i = i - 1;
      result = partitions[i];
      long zeroOrOne = Math.round(((float) input - partitions[i])
          / partitions[i]);
      // each step means (partitions[i] / 1024)
      result += zeroOrOne * partitions[i];
    }
    return result;
  }
}
