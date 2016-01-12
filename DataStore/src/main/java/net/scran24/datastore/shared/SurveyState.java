/*
This file is part of Intake24.

© Crown copyright, 2012, 2013, 2014.

This software is licensed under the Open Government Licence 3.0:

http://www.nationalarchives.gov.uk/doc/open-government-licence/
*/

package net.scran24.datastore.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public enum SurveyState implements IsSerializable {
	NOT_INITIALISED,
	SUSPENDED,
	ACTIVE
}
