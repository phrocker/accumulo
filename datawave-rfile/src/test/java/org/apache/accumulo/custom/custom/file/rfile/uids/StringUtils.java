/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.custom.custom.file.rfile.uids;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A class for general String utilities
 *
 */
public class StringUtils {

  /**
   * An empty, constant, zero-length string
   */
  public static final String EMPTY_STRING = "";

  public static String[] split(String str, char delimiter, boolean includeEmptyStrings) {
    List<String> strings = new ArrayList<>();
    for (String string : splitIterable(str, delimiter, includeEmptyStrings)) {
      strings.add(string);
    }
    return strings.toArray(new String[strings.size()]);
  }

  public static Iterable<String> splitIterable(String str, char delimiter,
      boolean includeEmptyStrings) {
    return new SplitIterable(str, delimiter, includeEmptyStrings);
  }

  /**
   * This class will provide an iterator over the splits
   *
   */
  public static class SplitIterable implements Iterable<String>, Iterator<String> {
    protected String str;
    protected char delimiter;
    protected boolean includeEmptyStrings;
    protected int fromIndex;
    protected int toIndex;
    protected String next;

    public SplitIterable(String str, char delimiter, boolean includeEmptyStrings) {
      this(str, delimiter, includeEmptyStrings, true);
    }

    protected SplitIterable(String str, char delimiter, boolean includeEmptyStrings,
        boolean getNext) {
      this.str = str;
      this.delimiter = delimiter;
      this.includeEmptyStrings = includeEmptyStrings;
      fromIndex = 0;
      toIndex = str.indexOf(delimiter);
      if (getNext) {
        getNext();
      }
    }

    @Override
    public Iterator<String> iterator() {
      return this;
    }

    protected void getNext() {
      next = null;
      while (toIndex >= 0 && next == null) {
        if (includeEmptyStrings || fromIndex < toIndex) {
          next = str.substring(fromIndex, toIndex);
        }
        fromIndex = toIndex + 1;
        toIndex = str.indexOf(delimiter, fromIndex);
      }
      if (next == null) {
        int strLen = str.length();
        if (includeEmptyStrings) {
          strLen++;
        }
        if (fromIndex < strLen) {
          next = str.substring(fromIndex);
        }
        fromIndex = strLen;
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public String next() {
      if (next == null) {
        throw new NoSuchElementException("No elements left");
      }
      String returnNext = next;
      getNext();
      return returnNext;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove");
    }

  }

  public static String[] split(String str, char delimiter) {
    List<String> strings = new ArrayList<>();
    for (String string : splitIterable(str, delimiter)) {
      strings.add(string);
    }
    return strings.toArray(new String[strings.size()]);
  }

  public static Iterable<String> splitIterable(String str, char delimiter) {
    // first trim the delimiters off the end
    int len = str.length();
    while (len > 0 && str.charAt(len - 1) == delimiter) {
      len--;
    }
    if (len < str.length()) {
      str = str.substring(0, len);
    }
    return splitIterable(str, delimiter, true);
  }

  public static String[] split(String str, String regex) {
    if (regex.length() == 1) {
      char c = regex.charAt(0);
      if (!isEscapeRequired(c)) {
        return split(str, c);
      }
    } else if (regex.length() == 2 && regex.charAt(0) == '\\') {
      char c = regex.charAt(1);
      if (isEscapableLiteral(c)) {
        return split(str, c);
      }
    }
    return str.split(regex);
  }

  public static Iterable<String> splitIterable(String str, String regex) {
    if (regex.length() == 1) {
      char c = regex.charAt(0);
      if (!isEscapeRequired(c)) {
        return splitIterable(str, c);
      }
    } else if (regex.length() == 2 && regex.charAt(0) == '\\') {
      char c = regex.charAt(1);
      if (isEscapableLiteral(c)) {
        return splitIterable(str, c);
      }
    }
    return Arrays.asList(str.split(regex));
  }

  /**
   * This class is the same as SplitIterable except that only the indexed strings specified are
   * returned.
   *
   */
  public static class SubSplitIterable extends SplitIterable
      implements Iterable<String>, Iterator<String> {
    int stringIndex;
    int[] indexesToReturn;
    int indexesIndex;

    public SubSplitIterable(String str, char delimiter, boolean includeEmptyStrings,
        int[] indexes) {
      super(str, delimiter, includeEmptyStrings, false);
      indexesToReturn = indexes;
      indexesIndex = 0;
      stringIndex = -1;
      getNext();
    }

    protected void getNext() {
      next = null;
      // if we have more to return
      if (indexesIndex < indexesToReturn.length) {
        // get the next string index to return
        int nextIndex = indexesToReturn[indexesIndex];
        indexesIndex++;

        int nextFrom = -1;
        int nextTo = -1;
        int strLen = str.length();

        // while we have more to get and we have not found our string
        while (fromIndex < strLen && stringIndex < nextIndex) {
          stringIndex++;
          nextFrom = -1;
          while (toIndex >= 0 && nextFrom < 0) {
            if (includeEmptyStrings || fromIndex < toIndex) {
              nextFrom = fromIndex;
              nextTo = toIndex;
            }
            fromIndex = toIndex + 1;
            toIndex = str.indexOf(delimiter, fromIndex);
          }
          if (nextFrom < 0) {
            int strEnd = strLen;
            if (includeEmptyStrings) {
              strEnd++;
            }
            if (fromIndex < strEnd) {
              nextFrom = fromIndex;
              nextTo = str.length();
            }
            fromIndex = strEnd;
          }
        }

        // if we are at the next string index, and we have a string to return, then return it
        if (stringIndex == nextIndex && fromIndex >= 0) {
          next = str.substring(nextFrom, nextTo);
        }
      }
    }
  }

  public static String[] split(String str, char delimiter, boolean includeEmptyStrings,
      int[] indexesToReturn) {
    String[] strings = new String[indexesToReturn.length];
    int index = 0;
    for (String string : splitIterable(str, delimiter, includeEmptyStrings, indexesToReturn)) {
      strings[index++] = string;
    }
    return strings;
  }

  public static Iterable<String> splitIterable(String str, char delimiter,
      boolean includeEmptyStrings, int[] indexesToReturn) {
    return new SubSplitIterable(str, delimiter, includeEmptyStrings, indexesToReturn);
  }

  public static Iterable<String> splitIterable(String str, char delimiter, int[] indexesToReturn) {
    // first trim the delimiters off the end
    int len = str.length();
    while (len > 0 && str.charAt(len - 1) == delimiter) {
      len--;
    }
    if (len < str.length()) {
      str = str.substring(0, len);
    }
    return splitIterable(str, delimiter, true, indexesToReturn);
  }

  public static String[] split(String str, String regex, int[] indexesToReturn) {
    if (regex.length() == 1) {
      char c = regex.charAt(0);
      if (!isEscapeRequired(c)) {
        return split(str, c, indexesToReturn);
      }
    } else if (regex.length() == 2 && regex.charAt(0) == '\\') {
      char c = regex.charAt(1);
      if (isEscapableLiteral(c)) {
        return split(str, c, indexesToReturn);
      }
    }

    // did not have time to make this part more efficient
    String[] values = str.split(regex);
    String[] returnValues = new String[indexesToReturn.length];
    for (int i = 0; i < indexesToReturn.length; i++) {
      if (indexesToReturn[i] < values.length) {
        returnValues[i] = values[indexesToReturn[i]];
      }
    }
    return returnValues;
  }

  public static String[] split(String str, char delimiter, int[] indexesToReturn) {
    String[] strings = new String[indexesToReturn.length];
    int index = 0;
    for (String string : splitIterable(str, delimiter, indexesToReturn)) {
      strings[index++] = string;
    }
    return strings;
  }

  /**
   * The character is reserved (i.e. required to be escaped) is it is one of $()*+.?[\^{|
   */
  static boolean isEscapeRequired(char c) {
    return (c == '$' || c == '(' || c == ')' || c == '*' || c == '+' || c == '.' || c == '?'
        || c == '[' || c == '\\' || c == '^' || c == '{' || c == '|');
  }

  /**
   * The character is escapable if it is not a digit or letter
   */
  static boolean isEscapableLiteral(char c) {
    return (!(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z'));
  }

  public static String[] trimAndRemoveEmptyStrings(String[] values) {
    List<String> newValues = null;
    for (int i = 0; i < values.length; i++) {
      String value = values[i].trim();
      if (value.isEmpty()) {
        if (newValues == null) {
          newValues = new ArrayList<>();
          newValues.addAll(Arrays.asList(values).subList(0, i));
        }
      } else if (value.length() != values[i].length()) {
        if (newValues == null) {
          newValues = new ArrayList<>();
          newValues.addAll(Arrays.asList(values).subList(0, i));
        }
        newValues.add(value);
      } else if (newValues != null) {
        newValues.add(value);
      }
    }
    if (newValues != null) {
      return newValues.toArray(new String[newValues.size()]);
    } else {
      return values;
    }
  }

  public static String[] deDupStringArray(String[] values) {
    if (values == null) {
      return null;
    }
    Set<String> stringArraySet = new HashSet<>(Arrays.asList(values));

    if (stringArraySet.size() == values.length) {
      return values;
    }

    return stringArraySet.toArray(new String[stringArraySet.size()]);
  }

  public static String substringAfterLast(String str, String separator) {
    int index = str.lastIndexOf(separator);
    if (index >= 0) {
      str = str.substring(index + 1);
    }

    return str;

  }
}
