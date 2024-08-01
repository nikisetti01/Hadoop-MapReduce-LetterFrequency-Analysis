# Letter Frequency Count with Hadoop MapReduce

Welcome to the Letter Frequency Count application using Hadoop MapReduce! This project demonstrates how to count letter frequencies in a text using two different MapReduce techniques and perform analysis on text data in multiple languages. The project is organized into several components for clarity.

## 📁 Project Structure

1. **Java MapReduce Code**:
   - **`LetterFrequencyMapReduce.java`**: Implements MapReduce functionality to count letter frequencies.
     - **InMapperCombiner**: Utilizes an in-mapper combiner to optimize performance.
     - **Classic Combiner**: Uses the classic combiner technique.

2. **Python Analysis**:
   - **`performance_analysis.py`**: Analyzes Hadoop application performance using various HDFS and YARN parameters.
   - **`language_comparison.py`**: Compares Latin text with texts in Romance languages to determine linguistic affinities and differences.

## 💻 Java MapReduce Code

### `LetterFrequencyMapReduce.java`

This file contains two implementations of the MapReduce job for counting letter frequencies:

- **InMapperCombiner**: 
  - **Purpose**: Reduces intermediate data during the map phase, optimizing performance.
  - **Usage**: The mapper combines intermediate results within the map function.

- **Classic Combiner**:
  - **Purpose**: Aggregates intermediate results before the reduce phase.
  - **Usage**: The combiner is used as a separate phase between map and reduce.

#### How It Works

1. **Mapper**: Processes input text to produce letter frequency counts.
2. **Combiner**: Aggregates results to reduce data volume.
3. **Reducer**: Finalizes the count and outputs the total letter frequencies.

## 📊 Python Analysis

### `performance_analysis.py`

This script evaluates the performance of the Hadoop MapReduce job based on various parameters:

- **HDFS Parameters**: Analyzes the efficiency of Hadoop Distributed File System settings.
- **YARN Parameters**: Assesses the impact of YARN settings on job performance.

**Objective**: Identify performance bottlenecks and optimize configuration settings.

### `language_comparison.py`

This script performs a comparative analysis of texts in Latin and Romance languages:

- **Text Analysis**: Uses data from texts of the *Aeneid* in various Romance languages and Latin.
- **Statistical Comparison**: Identifies similarities and differences, determining which Romance language is closest to Latin.

**Objective**: Provide insights into linguistic evolution and affinities over time.
