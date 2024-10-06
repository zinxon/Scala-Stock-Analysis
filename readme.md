# Scala Spark Stock Analysis

This project demonstrates the use of Apache Spark with Scala to analyze stock data, specifically focusing on Apple (AAPL) stock prices.

## Project Structure

The project consists of two main Scala files:

1. `Main.scala`: Contains the main application logic and data processing functions.
2. `FirstTest.scala`: Includes unit tests for the functions in the Main object.

## Features

- Read and process CSV stock data
- Calculate highest closing prices per year
- Perform various DataFrame operations and transformations
- Unit tests for key functions

## Dependencies

This project uses the following main dependencies:

- Scala 2.13.15
- Apache Spark 3.5.0
- ScalaTest 3.2.19 (for testing)

For a complete list of dependencies, please refer to the `build.sbt` file.

## How to Run

1. Ensure you have Scala and SBT (Scala Build Tool) installed on your system.
2. Clone this repository.
3. Navigate to the project directory.
4. Run the following command to execute the main application:

   ```
   sbt run
   ```

5. To run the tests, use the following command:

   ```
   sbt test
   ```

## Main Functions

### `highestClosingPricesPerYear`

This function calculates the highest closing price for each year in the dataset.

## Tests

The `FirstTest` class contains unit tests for the main functions. It uses ScalaTest's FunSuite for writing and running tests.
