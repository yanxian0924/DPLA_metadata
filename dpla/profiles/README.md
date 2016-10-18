# What is this?

These profiles are from the project work done in Fall 2015.  We're not sure what all we can use it for.

collProfile files have the following structure:
```
object		{4}
	collection		{4}
	collMetadataDetail		{5}
	duplicated_item
	itemDetail		[count of items]
		item1		{2}
			itemId	
				attrVolume		{25}
					description	
					creator	
					rightsHolder	
					collection	
					spatialCoords	
					genre	
					relation	
					displayDate	
					extent	
					contributor	
					alternative	
					dataProvider	
					subject	
					publisher	
					replaces	
					language	
					rights	
					temporal	
					format	
					provider	
					title	
					spatialName	
					identifier	
					type	
					replacedBy	
```

collectionData files have the following structure:

```
object		{3}
	collection		{4}
		title
		@id	
		id	
		description
	collMetadataDetail		{5}
		itemCount	
		dateProfiled
		Sourc
		collectionVolume		{25}
			creator	
			rightsHolder	
			relation	
			displayDate	
			contributor	
			alternative	
			dataProvider	
			subject	
			title	
			temporal	
			provider	
			type	
			description	
			format	
			collection	
			spatialCoords	
			replacedBy	
			extent	
			genre	
			publisher	
			replaces	
			language	
			rights	
			spatialName	
			identifier	
		collectionUsage		{25}
			creator	
			rightsHolder	
			relation	
			displayDate	
			contributor	
			alternative	
			dataProvider	
			subject	
			title	
			temporal	
			provider	
			type	
			description	
			format	
			collection	
			spatialCoords	
			replacedBy	
			extent	
			genre	
			publisher	
			replaces	
			language	
			rights	
			spatialName	
			identifier	
	duplicated_item
```
