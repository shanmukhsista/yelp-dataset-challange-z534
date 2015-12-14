use strict;
use warnings;
use Set::Object qw(set);
sub openAndReadFile{
    my $outFileName = "task1eval.csv";
    open(my $fhw, '>', $outFileName) or die "Could not open file '$outFileName' $!";
    my $filename = shift;
    open(my $fh, '<:encoding(UTF-8)', $filename)
      or die "Could not open file '$filename' $!";
    my $headerRow = <$fh>;
    my $totalCorrect = 0 ;
    my $total = 0 ;
    while (my $row = <$fh>) {
      my @outRow = ();
      chomp $row;
      #split row on comma
      my @cols = split( ",", $row);
      #print join(",", @cols) . "\n";
     push @outRow, $cols[0];
     my $actualCategories = set();
     my $acString = $cols[1];
     #split category string by #
     foreach my $cat (split("#",$acString)) {
       $actualCategories -> insert($cat);
     }
     #get add all predicted categories into another map.
     #cols are 3, 5, 7
     #print "\n". $actualCategories . "\n";
     my $predictedCats = set();


     my $pcString = $cols[3];

     #split category string by #
     if ( defined $pcString){
      foreach my $cat (split("#",$pcString)) {
               $predictedCats -> insert($cat);
          }
     }

     $pcString = $cols[5];
          #split category string by #
       if ( defined $pcString){
           foreach my $cat (split("#",$pcString)) {
                    $predictedCats -> insert($cat);
               }
          }
     $pcString = $cols[7];
           #split category string by #
       if ( defined $pcString){
           foreach my $cat (split("#",$pcString)) {
                    $predictedCats -> insert($cat);
               }
          }

     #print $predictedCats . "\n";


     #get total categories for business
     my $totalCats = $actualCategories -> size();

       # now for each actual category, check if it has been predicted.
     my $remaining = $actualCategories -> difference($predictedCats) ;
     my $incorrectPredictions = $remaining -> size();
     my $correct = $totalCats - $incorrectPredictions;
     if ( $correct > 0 ){
        $totalCorrect = $totalCorrect + 1 ;
     }
     $total = $total + 1 ;
     print $fhw "$cols[0],$correct,$incorrectPredictions,$totalCats\n" ;

    }
    print $fhw "Accuracy is " . (($totalCorrect*100.0)/($total)) . "%.\n"

}
#Open the task1-results.csv file and print line by line.
openAndReadFile("task1-results.csv");