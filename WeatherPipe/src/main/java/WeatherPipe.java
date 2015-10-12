import org.apache.commons.cli;


// create Options object
Options options = new Options();

// add t option
options.addOption("t", false, "display current time");

try {
    // parse the command line arguments
    CommandLine line = parser.parse( options, args );

    // validate that block-size has been set
    if( line.hasOption( "block-size" ) ) {
        // print the value of block-size
        System.out.println( line.getOptionValue( "block-size" ) );
    }
}
catch( ParseException exp ) {
    System.out.println( "Unexpected exception:" + exp.getMessage() );
}
