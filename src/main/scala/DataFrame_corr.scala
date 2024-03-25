import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object DataFrame_corr {

  def main(args: Array[String]): Unit = {
    //fichier code Postaux.csv
    // Le fichier contient 39193 lignes et 6 colonnes : Code_commune_INSEE, Nom_commune, Code_postal, Libelle_acheminement, Ligne_5, coordonnees_gps
    //Combien de colonnes contient-il ?
    //Comment ces colonnes sont-elles distribuées ?
    //6 colonnes : Code_commune_INSEE, Nom_commune, Code_postal, Libelle_acheminement, Ligne_5, coordonnees_gps
    //Quelle sont les types des colonnes du fichier ?
    //Code_commune_INSEE : String,
    // Nom_commune : String,
    // Code_postal : String,
    // Libelle_acheminement : String,
    // Ligne_5 : String nullable,
    // coordonnees_gps : tuple double nullable

    val spark = SparkSession.builder
      .appName("Spark df tp")
      .master("local[*]")
      .getOrCreate()

    // Lisez le fichier des codes postaux avec Spark et affichez son contenu dans la console.

    val filePath = "src/main/ressources/codesPostaux.csv"

    val df = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .csv(filePath)

    df.show()

    //Quel est le schéma du fichier ?
    println("Schema du fichier :")
    df.printSchema()
    //tout les attribut sont au format string nullable

    //Affichez le nombre de communes.
    val res2 = df.count()
    println(s"Nombre de communes : ${res2}")

    //Affichez le nombre de communes qui possèdent l’attribut Ligne_5

    val res3 = df
      .filter("Ligne_5 is not null")
      .count()
    println(s"Nombre de communes qui possèdent l'attribut Ligne_5 : ${res3}")

    //Ajoutez aux données une colonne contenant le numéro de département de la commune.
    println("Ajout de la colonne num_departement :")
    val dfWithDept = df.withColumn("departement", substring(col("Code_commune_INSEE"), 1, 2))
    dfWithDept.show()


    //Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”,
    //ayant pour colonne
    // Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
    println("Ecriture du fichier commune_et_departement.csv :")

    val writePath = "src/main/ressources/commune_et_departement"
    val dfCommuneEtDepartement = dfWithDept.select("Code_commune_INSEE", "Nom_commune", "Code_postal", "departement")

    dfCommuneEtDepartement
      .orderBy("Code_postal")
      .coalesce(1) //pour avoir un seul fichier
      .write
      .option("header", "true")
      .option("sep", ";")
      .mode("overwrite")
      .csv(writePath)
    // le resultat est dans le fichier PART-00000 du dossier commune_et_departement

    //pour avoir un seul fichier avec le bon nom
    //on recupere le fichier, on le deplace et le renomme
    val RENAMING = true

    if (RENAMING) {
      val fs = FileSystem.get(new Configuration())
      val file = fs.globStatus(new Path(writePath + "/part*"))(0).getPath.toString
      fs.rename(new Path(file), new Path(writePath + ".csv"))
      fs.delete(new Path(writePath), true)
    }

    //Affichez les communes du département de l’Aisne.

    //lecture de la db departement-france de data.gouv.fr
    val dept_filePath = "src/main/ressources/departements-france.csv"
    val dept_df = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv(dept_filePath)

    println("Affichage des communes du département de l'Aisne :")
    //on recuper le code du departement de l'Aisne
    val num_dept = dept_df
      .filter("nom_departement = 'Aisne'")
      .select("code_departement")
      .first()(0)
    //on affiche les communes du departement de l'Aisne
    dfCommuneEtDepartement
      .filter(s"departement = ${num_dept}")
      .show()

    //Quel est le derpartment qui possède le plus de communes ?
    println("Departement qui possède le plus de communes :")
    val DeptWithMostCommune = dfCommuneEtDepartement
      .groupBy("departement")
      .count()
      .orderBy(col("count").desc)
      .first()

    //on recupere le nom du departement
    val nom_deptWithMostCommune = dept_df
      .filter(s"code_departement = ${DeptWithMostCommune(0)}")
      .select("nom_departement")
      .first()(0)

    //on affiche le resultat
    println(s"Le departement ${nom_deptWithMostCommune} (${DeptWithMostCommune(0)}) avec " +
      s"${DeptWithMostCommune(1)} communes est le Departement qui possède le plus de communes")


  }


}
