#!/usr/bin/perl
use warnings;
my $other_branch = "upstream/branch-3.0";
my $last_commit = "79444d6d6f1e63f28a27420b3165e1115c35c687";
my $other_git_log = `git log --format=oneline $other_branch...$last_commit`;
my $long_git_log = `git log --decorate --color=always -p $other_branch...$last_commit`;
my @commits_code = reverse(split('\n\S*commit\s+', $long_git_log));
my $commits_code_size = $#commits_code;
my @commits_desc = reverse(split('\n', $other_git_log));
my $commits_desc_size = $#commits_desc;
my $new_changes = "";
# Process the commits
if ($commits_desc_size != $commits_code_size) {
    print "Error $commits_desc_size != $commits_code_size";
    exit;
}
foreach my $i (0..$#commits_desc) {
    my $commit_desc = $commits_desc[$i];
    my $commit_code = $commits_code[$i];
    print "Code:\n$commit_code\n";
    print "Merge commit $commit_desc? [y/n]";
    while (my $input = <>) {
	chomp($input);
	if ($input eq 'y') {
	    # Extract the info we need for the log for commits of [SPARK-...]
	    if ($commit_desc =~ /([a-f0-9]{40})\s+\[(SPARK-\d+)\](.*\])\s*(.+?)$/) {
		my $commit = $1;
		my $jira = $2;
		my $desc = $4;
		$new_changes = "$new_changes\n$jira\t $desc";
		print `git cherry-pick $commit`;
	    # Sometimes we have hotfixes and the like
	    } elsif ($commit_desc =~ /([a-f0-9]{40})\s+(.+)/) {
		my $commit = $1;
		my $desc = $2;
		$new_changes = "$new_changes\nUNSET\t $desc";
		print `git cherry-pick $commit`;
	    } else {
		print "Can't handle $commit_desc please merge manually";
	    }
	    last;
	} elsif ($input eq 'n') {
	    last;
	} else {
	    print "Invalid answer $input\n";
	}
    }
}
print "Please update APPLE_CHANGES with:\n$new_changes\n";
