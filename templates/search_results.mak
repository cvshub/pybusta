<%include file="header.mak"/>
<table>
	<tr>
	<td>id</td><td width=20%%>Автор</td><td>Название</td><td>Язык</td><td>Размер</td>
	</tr>
	% for item in items:
	<tr>
	   <td> ${item['id']} </td>
	   <td> ${item['author']} </td>
	   <td> 
	   		<a href="/get/${item['id']}">${item['title']}</a>
	   </td>
	   <td> ${item['language']} </td>
	   <td> ${item['size']} </td>
	</tr>   
	% endfor
</table>
<%include file="footer.mak"/>
