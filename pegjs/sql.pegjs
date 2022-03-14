start =
	select_statement

columns =
	[a-zA-Z0-9]+
	/ [a-zA-Z0-9]+"," (columns)

select_statement =
	select:"SELECT"i columns:(columns)+ from:"FROM"i table:[a-z]+
