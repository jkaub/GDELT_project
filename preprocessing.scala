import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import spark.implicits._
import sys.process._
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import java.io.File
import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.net.HttpURLConnection
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.sql.SQLContext
import scala.io.Source

val AWS_ID = "########"
val AWS_KEY = "########"
val awsClient = new AmazonS3Client(new BasicAWSCredentials(AWS_ID, AWS_KEY))


// Setting S3 configurations
sc.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID)
sc.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY)
sc.hadoopConfiguration.set("fs.s3a.connection.maximum","10000000")

//---------------------------------------------------------------------------//
/***************************** FUNCTIONS  ************************************/
//---------------------------------------------------------------------------//

// ####################### EXPORT/MENTIONS ################################# //

// Function to load an export_file into dataframe
def processExportFiles(pathSuffix: String): org.apache.spark.sql.DataFrame = {

  // Load rdd
  val rdd = sc.binaryFiles("s3a://gdeltparis/"+pathSuffix). // charger quelques fichers via une regex
     flatMap {  // decompresser les fichiers
         case (name: String, content: PortableDataStream) =>
            val zis = new ZipInputStream(content.open)
            Stream.continually(zis.getNextEntry).
                  takeWhile(_ != null).
                  flatMap { _ =>
                      val br = new BufferedReader(new InputStreamReader(zis))
                      Stream.continually(br.readLine()).takeWhile(_ != null)
                  }
      }.cache

  // Transforming rdd in dataframe and getting only the columns we want
  val df = rdd.toDF.withColumn("_tmp", split($"value", "\t")).select(
    $"_tmp".getItem(0).as("EventID"),
    $"_tmp".getItem(1).as("Date"),
    $"_tmp".getItem(6).as("Actor1Name"),
    $"_tmp".getItem(7).as("Actor1Country"),
    $"_tmp".getItem(16).as("Actor2Name"),
    $"_tmp".getItem(17).as("Actor2Country")
  ).drop("_tmp","value")


  // Udfs for column creation

  // For Column "Contains_actor2"
  def udfContains = udf{(actor1: String, actor2: String) =>
    if (actor1 == "" || actor2 == "") 0 else 1
  }

  def udfYear = udf{(date: String) => date.slice(0,4)}

  def udfMonth = udf{(date: String) => date.slice(4,6)}

  // Creating columns and filtering
  val dfExports = df
    .withColumn("Year", udfYear($"Date"))
    .withColumn("Month", udfMonth($"Date"))
    .withColumn("Contains_2actors", udfContains($"Actor1Name",$"Actor2Name"))
    .filter(($"Year"==="2018") && ($"Actor1Name"!="" || $"Actor2Name"!=""));

  // Remove zip files in ./tmp
  //Process("rm "+tmp_path+file).lineStream;

  dfExports
}

// Function to load a mentions_file into dataframe
def processMentionsFiles(pathSuffix: String): org.apache.spark.sql.DataFrame = {


  // Load rdd
  val rdd = sc.binaryFiles("s3a://gdeltparis/"+pathSuffix). // load zip files
     flatMap {    // decompresser les fichiers
         case (name: String, content: PortableDataStream) =>
            val zis = new ZipInputStream(content.open)
            Stream.continually(zis.getNextEntry).
                  takeWhile(_ != null).
                  flatMap { _ =>
                      val br = new BufferedReader(new InputStreamReader(zis))
                      Stream.continually(br.readLine()).takeWhile(_ != null)
                  }
      }.cache

  // Transforming rdd in dataframe and getting only the columns we want
  val df = rdd.toDF.withColumn("_tmp", split($"value", "\t")).select(
    $"_tmp".getItem(0).as("EventID"),
    $"_tmp".getItem(2).as("MentionDate"),
    $"_tmp".getItem(13).as("Tone"),
    $"_tmp".getItem(14).as("Language")
  ).drop("_tmp","value");


  // Udf for column "Language"
  def udfLang = udf{(mentionDocTranslation: String) =>
    if (mentionDocTranslation == "") "eng" else mentionDocTranslation.slice(6,9)
  }

  def udfMonth = udf{(date: String) => date.slice(4,6)}

  def udfYear = udf{(date: String) => date.slice(0,4)}

  // Creating columns and filtering for 2018
  val dfMentions = df
    .withColumn("MentionYear", udfYear($"MentionDate"))
    .withColumn("MentionMonth", udfMonth($"MentionDate"))
    .withColumn("Language", udfLang($"Language"));
  // Remove csv files in ./tmp

  //Process("rm "+tmp_path+file).lineStream;

  dfMentions
}

def concatEventMention(eventsAll: org.apache.spark.sql.DataFrame,
  mentionsAll: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {

	// Concatenate events and mentions DF joining on EventID - "long version", i.e. key = EventID x MentionID
	val concatInit = eventsAll.join(mentionsAll, usingColumns = Seq("EventID"), joinType = "left_outer")

	// From the long version, concatenate relevant fields of Mentions into a single structure, then group by key
	val concatFinal = concatInit
	.select($"EventID", $"Date", $"Year", $"Month", $"Actor1Name", $"Actor1Country", $"Actor2Name", $"Actor2Country", $"Contains_2actors", struct($"MentionDate", $"MentionYear", $"MentionMonth", $"Tone", $"Language")
		.alias("Mention"))
	.groupBy("EventID", "Date", "Year", "Month", "Actor1Name", "Actor1Country", "Actor2Name", "Actor2Country", "Contains_2actors")
	.agg(collect_list("Mention")
		.alias("Mention"))

	concatFinal
}

// ############################## GKG ###################################### //

def processGKGFiles(pathSuffix: String): org.apache.spark.sql.DataFrame = {

  // Read zipped file in RDD
  val rdd = sc.binaryFiles("s3a://gdeltparis/"+pathSuffix). // load zip files
   flatMap {  // decompresser les fichiers
       case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
                takeWhile(_ != null).
                flatMap { _ =>
                    val br = new BufferedReader(new InputStreamReader(zis))
                    Stream.continually(br.readLine()).takeWhile(_ != null)
                }
    }

  // Transforming rdd in dataframe and getting only the columns we want
  val df = rdd.toDF.withColumn("_tmp", split($"value", "\t")).select(
    $"_tmp".getItem(0).as("ArticleID"),
    $"_tmp".getItem(9).as("Locations"),
    $"_tmp".getItem(15).as("Tone"),
    $"_tmp".getItem(7).as("Themes")
  ).drop("_tmp","value")
  // UdFs to transform columns
  def udfDate = udf{(dateString: String) =>
    dateString.slice(0,8)
  }

  val regexLocation = """1\#([a-zA-Z\s]*)\#""".r

  def udfLocation = udf{(locationString: String) =>
    if (locationString == null) {
      Array[String]()
    }else {
      regexLocation.findAllMatchIn(locationString)
      .map(_.toString.trim.replace("1#", "")
      .replace("#", ""))
      .toArray
    }

  }

  def udfTone = udf{(tone: String) =>
    if (tone == null) "" else tone.split(",")(0)
  }

  // Creating map of theme categories for GKG theme
  val listSource = Source.fromFile("/home/hadoop/cat_corr.csv").getLines().toList
  val mapCatTheme = listSource.map(
                            t => t.replace(""""""", "")
                          ).map(
                            t => t.split(",")
                          ).collect{
                            case Array(k,v) => k.toString->v.toString
                          }.toMap

  def udfCat = udf{(theme: String) =>
    if (theme == null) {
      Array[String]()
    } else {
      theme.split(";")
      .map(theme => mapCatTheme.getOrElse(theme, "other"))
      .distinct
    }
  }
  // Creating columns and filtering
  val dfGKG = df
    .withColumn("Date", udfDate($"ArticleID"))
    .withColumn("LocationsList", udfLocation($"Locations"))
    .withColumn("GkgTone", udfTone($"Tone"))
    .withColumn("Category", udfCat($"Themes"))
    .select("ArticleID", "Date", "LocationsList", "GkgTone", "Category")

  dfGKG
}

//---------------------------------------------------------------------------//
/***************************** PROCESSING FILES  *****************************/
//---------------------------------------------------------------------------//

// Build spark session
val spark = SparkSession.builder().getOrCreate()

// ####################### EXPORT/MENTIONS ################################# //

val exportsEN = processExportFiles("tmp/*.export.CSV.zip").cache
val exportsTR = processExportFiles("tmp_tr/*.export.CSV.zip").cache

val mentionsEN = processMentionsFiles("tmp/*.mentions.CSV.zip").cache
val mentionsTR = processMentionsFiles("tmp_tr/*.mentions.CSV.zip").cache

// Append English files to Translation files
val exportsAll = exportsEN.union(exportsTR).cache
val mentionsAll = mentionsEN.union(mentionsTR).cache

// Merge exports and mentions -> save as json on S3
val concatFinal = concatEventMention(exportsAll, mentionsAll).cache
concatFinal.write.json("s3a://gdeltparis/event_mention_processed/json_files")

// ############################## GKG ###################################### //

val gkgEN = processGKGFiles("tmp_gkg/*.gkg.csv.zip")
val gkgTR = processGKGFiles("tmp_gkg_tr/*gkg.csv.zip")

// Save as json on S3
gkgEN.repartition(50).write.json("s3a://gdeltparis/gkg_proc_themes/EN")
gkgTR.repartition(50).write.json("s3a://gdeltparis/gkg_proc_themes/TR")
