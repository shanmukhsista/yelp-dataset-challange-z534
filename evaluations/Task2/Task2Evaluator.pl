use strict;
use warnings;
use Set::Object qw(set);
sub openAndReadFile{
    my $outFileName = "task2eval.csv";
    open(my $fhw, '>', $outFileName) or die "Could not open file '$outFileName' $!";
    my $filename = shift;
    open(my $fh, '<:encoding(UTF-8)', $filename)
      or die "Could not open file '$filename' $!";
    my $headerRow = <$fh>;
    my $totalCorrect = 0 ;
    my $total = 0 ;
    my %userCategories = ();
    while (my $row = <$fh>) {
      my @outRow = ();
      chomp $row;
      #split row on comma
      my @cols = split( ",", $row);

      #print join(",", @cols) . "\n";
      my $userid = @cols[0];

     my $acString = $cols[2];
     #split category string by #
      if (defined $acString){
       if ( exists $userCategories{$userid}){
              my $actualCategories = $userCategories{$userid};
               foreach my $cat (split("#",$acString)) {
                     $actualCategories -> insert($cat);
               }
               $userCategories{$userid} = $actualCategories;
           }
           else{
                my $actualCategories = set();
                foreach my $cat (split("#",$acString)) {
                               $actualCategories -> insert($cat);
                }
                $userCategories{$userid} = $actualCategories;
           }

      }


#       # now for each actual category, check if it has been predicted.
#     my $remaining = $actualCategories -> difference($predictedCats) ;
#     my $incorrectPredictions = $remaining -> size();
#     my $correct = $totalCats - $incorrectPredictions;
#     if ( $correct > 0 ){
#        $totalCorrect = $totalCorrect + 1 ;
#     }
#     $total = $total + 1 ;
#     print $fhw "$cols[0],$correct,$incorrectPredictions,$totalCats\n" ;

    }


    print "size of map : " . scalar(%userCategories) .   "\n , ";
   #Now read the recommendations.csv file and check for recommendations.
    #print $fhw "Accuracy is " . (($totalCorrect*100.0)/($total)) . "%.\n"

      $filename = "recommendations.csv";

        open(my $fhr, '<:encoding(UTF-8)', $filename)
          or die "Could not open file '$filename' $!";
    my $recHeader = <$fhr>;
    my $retrieved = 0 ;
    my %recCategories = ();
    while (my $row = <$fhr>) {
          my @outRow = ();
          chomp $row;

     my $actualCategories = set();
             #split row on comma
          my @cols = split( ",", $row);

          #print join(",", @cols) . "\n";
          my $luserid = @cols[0];
          my $acString = $cols[2];
          if ( defined $acString){
          #split category string by #


           if ( exists $recCategories{$luserid}){
                             my $actualCategories = $recCategories{$luserid};
                              foreach my $cat (split("#",$acString)) {
                                                $actualCategories -> insert($cat);
                               }
                              $recCategories{$luserid} = $actualCategories;
                          }
                          else{
                               my $actualCategories = set();
                               foreach my $cat (split("#",$acString)) {
                                            $actualCategories -> insert($cat);
                               }
                              $recCategories{$luserid} = $actualCategories;
                      }

                     # print $recCategories{$luserid} . "\n ."

          }
           #




    }

    #Now we have the following maps :
    #recommendations Map -> Contains a set of categories recommended to the user.
    #$userCategoriesmap - contains a list of actual user categories
    #for each user, get everything and compute mean average precision.

    print "size of map : " . scalar(%recCategories) .   "\n , ";

  #Find an intersection of this set for this user to compute precision.  .
          foreach my $key (keys %recCategories)
          {
            if( exists $userCategories{$key}){
                     my $itemsRetrieved = ($recCategories{$key});
                          my $relevantDocuments = ($userCategories{$key}) -> size();
                          my  $relevantRetrieved = ($recCategories{$key}) -> intersection(($userCategories{$key}));
                            my $precision = ($relevantRetrieved -> size() * 1.0)/( ($recCategories{$key}) -> size() );
                            my $recall = ($relevantRetrieved -> size() * 1.0)/( ($userCategories{$key}) -> size() );

                            if ( $precision + $recall != 0){
                                 my $fmeasure   = ((2.0 * $precision * $recall)/($precision + $recall));
                                 print $fhw "$key,$precision,$recall,$fmeasure\n";

                            }
                            else{
                                print $fhw "$key,$precision,$recall,\n";
                            }


            }

          }
          close($fh);
          close($fhw);

}

#Read the base_user_reviews csv file and get a map of all the business categories rated by the user.
openAndReadFile("base_user_reviews.csv");